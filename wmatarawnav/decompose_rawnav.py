# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 07:13:54 2020

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np
from . import low_level_fns as ll

def decompose_segment_ff(rawnav,
                         segment_summary_,
                         max_fps = 73.3):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    segment_summary_: pd.DataFrame, defines segment start and end points
    max_fps: np.int or np.float, defines maximum speed above which values will be converted to NA.
        By default, approx 50 mph (73.3 fps)
    Returns
    -------
    freeflow_seg: pd.DataFrame, set of freeflow speed values at various percentiles. 
    """

    # Even though we lose last three points inside segment, this way we don't pick up points outside
    # This filter-calculate order appears opposite elsewhere, but is appropriate there.
    rawnav_fil = filter_to_segment(rawnav,
                                   segment_summary_)
    
    rawnav_fil = calc_rolling_vals(rawnav)
           
    freeflow_seg = (
        rawnav_fil
        .loc[lambda x: x.fps_next3 < max_fps, 'fps_next3']
        .quantile([0.01, 0.05, 0.10, 0.15, 0.25, 0.5, 0.75, 0.85, 0.90, 0.95, 0.99])
        .to_frame()
        .assign(mph = lambda x: x.fps_next3 / 1.467)
    )
    
    return(freeflow_seg)

def decompose_traveltime(
        rawnav,
        segment_summary_,
        stop_area_decomp,
        segment_ff_seg
    ):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft.
    segment_summary_: pd.DataFrame, defines segment start and end points
    stop_area_decomp: pd.DataFrame, subset of ranwav data within a stop area with column
        stop_area_phase indicating the phase of the vehicle at each stage.
    segment_ff_seg: np.int or np.float, the freeflow speed in feet per segment
    Returns
    -------
    travel_time_decomp: pd.DataFrame, decomposition of travel time in a segment. 
    """
    
    basic_decomp_agg = (
        stop_area_decomp
        # Note that we drop any stop_id grouping here, since this method just needs us to sum t_stop1s
        .groupby(['filename','index_run_start','stop_area_phase'])
        # While we can sum marginal values for t_stop1, if we do so for the accel phase, we'll
        # include a value outside of the segment. Instead, we subtract the min from the max value
        # to find the difference
        .agg({"secs_marg" : ['sum'],
              "sec_past_st": ['min','max'], 
              "odom_ft": ['min','max'],
              "fps_next3" : ['first','last'],
              "odom_ft_qj_stop" : ['first'],
              "start_date_time" :['first']})
        .pipe(ll.reset_col_names)
        .rename(columns = {'odom_ft_qj_stop_first':'odom_ft_qj_stop',
                           'start_data_time_first':'start_date_time'})
    )
  
    t_stop1_by_run = (
        basic_decomp_agg
        .loc[lambda x: x.stop_area_phase.isin(["t_stop1","t_stop",'t_l_initial'])]
        .filter(items = ['filename','index_run_start','stop_area_phase','secs_marg_sum'])
        .pivot_table(
            index = ['filename','index_run_start'], 
            columns = ['stop_area_phase'], 
            values = 'secs_marg_sum'
        )
        .reset_index()
    )
    
    # Rather than expanding this in a more complicated fashion (there's no pivot_table_spec option)
    # we just readd column if it's automatically dropped for lack of values
    if ('t_stop' not in t_stop1_by_run.columns):
        t_stop1_by_run= t_stop1_by_run.assign(t_stop = 0)
        
    # Calculate T_stop2
    
    # Filter to cases that stopped in any fashion
    basic_decomp_agg_fil = (
        basic_decomp_agg[
            basic_decomp_agg
            .groupby(['filename','index_run_start'])['stop_area_phase']
            .transform(lambda x: x.isin(['t_stop','t_stop1']).any())
        ]
    )
    
    # Note: One could filter to higher speed runs at this point, but the effect of using 
    # 95th percentile values below essentially serves the same function.
    
    decacc_by_run = (
        basic_decomp_agg_fil
        # TODO: If multi-stop segments are added, additional logic should be added here
        # to ensure phases are handled properly
        .assign(fpsps_phase = lambda x: (
            (x.fps_next3_last - x.fps_next3_first) 
            / (x.sec_past_st_max - x.sec_past_st_min)
            )
        )
        .assign(fpsps_phase = lambda x: abs(x.fpsps_phase))
        
    )
    
    t_decacc_by_seg = (
        decacc_by_run 
        .groupby(['stop_area_phase'])
        .agg({'fpsps_phase' : ['mean',
                               lambda x: x.quantile(.5),
                               lambda x: x.quantile(.90),
                               lambda x: x.quantile(.95)]})
        .pipe(ll.reset_col_names)
        # Several values are calculated for proofing purposes, but only one is used
        # for downstream calculations.
        .rename(columns = {"fpsps_phase_<lambda_0>" : "fpsps_phase_median",
                           "fpsps_phase_<lambda_1>" : "fpsps_phase_p90",
                           "fpsps_phase_<lambda_2>" : "fpsps_phase_p95"})
        .loc[lambda x: x.stop_area_phase.isin(['t_accel_phase','t_decel_phase'])]
        .assign(t_decacc = lambda x, ff = segment_ff_seg: abs(ff / (2 * x.fpsps_phase_p95)))
        .t_decacc
        .to_numpy()
        .sum()
    ) 
        
    # calc total secs
    rawnav_fil_seg = calc_rolling_vals(rawnav)

    rawnav_fil_seg = filter_to_segment(rawnav_fil_seg,
                                       segment_summary_)

    totals = (
        rawnav_fil_seg
        .groupby(['filename','index_run_start'])
        .agg({"odom_ft": [lambda x: max(x) - min(x)],
              "sec_past_st" : [lambda x: max(x) - min(x)]})
        .pipe(ll.reset_col_names)
        .rename(columns = {'odom_ft_<lambda>': 'odom_ft_seg_total',
                           'sec_past_st_<lambda>':'t_segment'})
    )

    # Join inputs together and calculate
    travel_time_decomp = (
        segment_summary_
        .merge(
            t_stop1_by_run,
            on = ['filename','index_run_start'],
            how = "left"
        )
        .merge(
            totals,
            on = ['filename','index_run_start'],
            how = "left"
        )
        .assign(
            flag_nostop = lambda x: (x.t_stop1.isna() & x.t_stop.isna()),
            flag_odometer_reset = lambda x: ((x.end_sec_segment- x.start_sec_segment) != x.t_segment)
        )
        .fillna({'t_stop1': 0,
                 't_stop' : 0,
                 't_l_initial': 0}) 
        .assign(
            ff_fps = lambda x, y = segment_ff_seg: y,
            t_ff = lambda x: x.odom_ft_seg_total / x.ff_fps
        )
        .assign(
            t_decacc = lambda x, y = t_decacc_by_seg: y,
            t_stop2 = lambda x: x.t_decacc + x.t_l_initial 
        )
        .assign(
            t_stop2 = lambda x: np.where(((x.t_stop1 == 0) & (x.t_stop == 0)),
                                         0,
                                         x.t_stop2)
        )
        .assign(
            t_traffic = lambda x: x.t_segment - x.t_ff - x.t_stop2 - x.t_stop1 - x.t_stop
        )
        .drop(columns = ['ff_fps'])
    )

    return(travel_time_decomp)
    
def decompose_stop_area(rawnav,
                        segment_summary_,
                        stop_index_fil,
                        stop_area_upstream_ft = 150,
                        stop_area_downstream_ft = 150):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    segment_summary_: pd.DataFrame, defines segment start and end points. Should already
        be filtered to the correct patterns relevant to segments
    stop_index_fil: pd.DataFrame, defines index points of stops
    stop_area_upstream_ft: float, number of feet upstream to define the stop area. Will not
        extend past the start of the segment even if a high value is chosen.
    stop_area_downstream_ft: float, number of feet upstream to define the stop area. Will not
        extend past the end of the segment even if a high value is chosen.
    Returns
    -------
    stop_area_decomp: pd.DataFrame, rawnav data with additional fields illustrating how 
    each run has been decomposed within the stop area, an input to other analyses.
    Notes
    -----
    This function creates phase identifers within a stop area to support further
    decomposition. These phase identifiers include:
        t_decel_phase = time from start of segment to zero speed (or door open)
        t_l_inital = time from first door open back to beginning of zero speed, if any
        t_stop1 = Stop zone door open time defined as first instance of door opening.
        t_stop = For vehicles that do not serve passengers, the time between the first time the 
        vehicle stops in the stop area to the time it begins moving for the last time in the stop
         area. 
        t_l_addl = time from first door close to acceleration
        t_accel_phase = time from acceleration to exiting stop zone (used to help estimate aacc
    """

    # Parameter Checks
    assert(len(rawnav) > 0), print("Halting, no stops provided in rawnav data")
    assert(len(segment_summary_) > 0), print("Halting, no stops provided in segment_summary_")
    assert(len(stop_index_fil) > 0), print("Halting, no stops provided in stop_index_fil")

    rawnav_fil_1 = filter_to_segment(rawnav,
                                     segment_summary_)

    # We'll also filter to those runs that have a match to the QJ stop while adding detail
    # about the QJ stop zone, just in case any of these runs behaved normally at segment ends
    # but oddly at the stop zone.
    rawnav_fil = (
        rawnav_fil_1
        .merge(
            stop_index_fil
            .filter(items = ['filename','index_run_start','odom_ft_qj_stop','stop_id']),
            on = ['filename','index_run_start'],
            how = "left"
        )
    )

    del rawnav_fil_1

    # Add Additional Metrics to Rawnav Data 
    # Downstream calculations will require certain values like the speed over the next interval
    rawnav_fil = calc_rolling_vals(rawnav_fil,
                                   groupvars = ['filename','index_run_start','stop_id'])
   
    # Filter to Stop Area
    rawnav_fil_stop_area_1 = (
        rawnav_fil
        .query('odom_ft >= (odom_ft_qj_stop - @stop_area_upstream_ft) & odom_ft <= (odom_ft_qj_stop + @stop_area_downstream_ft)')
        .reset_index()
    )
    
    # In the event that a ping ends up in two stop areas, we keep the last occurrence, as usually
    # we need more room on the upstream side. 
    rawnav_fil_stop_area_1 = (
        rawnav_fil_stop_area_1
        .loc[
            ~rawnav_fil_stop_area_1.duplicated(['filename','index_run_start','index_loc'], keep = "last")
        ]
    )
    
    # Add binary variables
    rawnav_fil_stop_area_2 = (
        rawnav_fil_stop_area_1
        .assign(
            door_state_closed=lambda x: x.door_state == "C",
            # note this returns False when fps_next is undefined. We'll have to handle this 
            # carefully in later stages, as we don't want to inadvertently signal this as a change
            # in status.
            veh_state_moving=lambda x: x.fps_next > 0                                                                                               
        )
    )
    
    # Add a sequential numbering that increments each time door changes in a run/segment combination   
    rawnav_fil_stop_area_2['door_state_changes'] = (
        rawnav_fil_stop_area_2
        .groupby(['filename','index_run_start','stop_id'])['door_state_closed']
        .transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    # We have to be more careful for vehicle state changes. At times, we'll get undefined speeds
    # (e.g., two pings have the same distance and time values) and this could be categorized 
    # as a change in state if we use the same approach as above. Instead, we'll create a separate
    # table without 'bad' speed records, run the calc on state changes,
    # join back to the original dataset, and then fill the missing values based on nearby ones.
    # Filling based on surrounding values is itself imperfect, but likely to be sufficient 
    # in many cases.
    veh_state = (
        rawnav_fil_stop_area_2
        .filter(items = ['filename','index_run_start','stop_id','index_loc','veh_state_moving'])
        .loc[~rawnav_fil_stop_area_2.fps_next.isnull()]
    )
    
    veh_state['veh_state_changes'] = (
            veh_state
            .groupby(['filename','index_run_start','stop_id'])['veh_state_moving']
            .transform(lambda x: x.diff().ne(0).cumsum())
    )

    rawnav_fil_stop_area_3 = (
        rawnav_fil_stop_area_2
        .merge(
            veh_state
            .drop(columns = ['veh_state_moving']),
            on = ['filename','index_run_start','stop_id','index_loc'],
            how = 'left'
        )
    )
    
    # Note that this could miss cases of transition where the null value for speed occurs
    # at a stop where passengers board/alight. However, if that's the case, we don't use 
    # these values anyhow.
    rawnav_fil_stop_area_3['veh_state_changes'] = (
        rawnav_fil_stop_area_3
        .groupby(['filename','index_run_start','stop_id'])['veh_state_changes']
        .transform(lambda x: x.ffill())
        .transform(lambda x: x.bfill())
    )
           
    # To identify the cases of the first door opening and last at each stop (needed for decomposition),
    # we'll summarize to a different dataframe and rejoin. 
    # The 'min' is almost always 2, but we're extra careful here in case the door is open at the 
    # start of the segment.
    # 'max' will be interesting - we'll add anything after the first door closing to the last reclosing
    # as signal delay, essentially.
    door_open_cases = (
        rawnav_fil_stop_area_3
        .loc[rawnav_fil_stop_area_3.door_state == "O"]
        .groupby(['filename','index_run_start','stop_id','door_state'])
        .agg({"door_state_changes" : ['min','max']})
        .pipe(ll.reset_col_names)
        .drop(columns = ['door_state'])
    )
       
    # Before we make use of the door_open_cases min and max files, we'll do a similar check on where 
    # the bus came to be not moving. The object namign is a little fuzzy here -- we'll call this
    # 'veh_stop' to distinguish that we're talking about the bus literally not moving, 
    # rather than something to do with a 'bus stop'. This helps with runs where the bus does not
    # stop at all.
    veh_stop_cases = (
        rawnav_fil_stop_area_3
        .loc[(~rawnav_fil_stop_area_3.veh_state_moving & rawnav_fil_stop_area_3.fps_next.notnull())]
        .groupby(['filename','index_run_start','stop_id','veh_state_moving'])
        .agg({"veh_state_changes" : ['min','max']})
        .pipe(ll.reset_col_names)
        .rename(columns = {"filename_" : "filename",
                          "index_run_start_": "index_run_start",
                          "stop_id_":"stop_id",
                          "veh_state_changes_min": "veh_stopped_min",
                          "veh_state_changes_max": "veh_stopped_max"})
        .drop(columns = ['veh_state_moving'])
    )
    
    # There will be nans remaining here from cases where bus did not stop or did not pick up 
    # passengers. This is okay, we'll handle these in a bit.

    rawnav_fil_stop_area_4 = (
        rawnav_fil_stop_area_3
        .merge(
            door_open_cases,
            on = ['filename','index_run_start','stop_id'],
            how = 'left'
        )
        .merge(
            veh_stop_cases,
            on = ['filename','index_run_start','stop_id'],
            how = 'left'
        )
    )
    
    # For convience in other downstream calcs,  we'll add flags to help with certain cases 
    # where vehicle didn't stop at all or doors didn't open.
    # These will be decomposed a little bit differently.
    # We'll reuse a table we made earlier since we've dealt with fps_next nulls here.
    veh_state_any_move = (
        veh_state 
        .drop(columns = ['veh_state_changes'])
    )
    
    veh_state_any_move['any_veh_stopped'] = (
        veh_state_any_move
        .groupby(['filename','index_run_start','stop_id'])['veh_state_moving']
        .transform(lambda x: any(~x))
    )

    rawnav_fil_stop_area_4 = (
        rawnav_fil_stop_area_4 
        .merge(
            veh_state_any_move
            .drop(columns = ['veh_state_moving']),
            on = ['filename','index_run_start','stop_id','index_loc'],
            how = "left"
        )
        # some NA's may appear after join. We'll usually fill to address these (see below),
        # but first, in the case where we know doors are open, we'll override and say the vehicle
        # is stopped.
        # Some additional work to prevent warnings generated from numpy/pandas conflicts on
        # elementwise comparison.
        .assign(any_veh_stopped = lambda x: np.where(x.any_veh_stopped.isnull().to_numpy() 
                                                      & (x.door_state.to_numpy() == "O"),  
                                                     True,
                                                     x.any_veh_stopped)
        )
    )
    
    rawnav_fil_stop_area_4['any_veh_stopped'] = (
        rawnav_fil_stop_area_4
        .groupby(['filename','index_run_start','stop_id'])['any_veh_stopped']
        .transform(lambda x: x.ffill())
        .transform(lambda x: x.bfill())
    )
        
    # Similar approach for door open but directly applied
    rawnav_fil_stop_area_4['any_door_open'] = (
        rawnav_fil_stop_area_4
        .groupby(['filename','index_run_start','stop_id'])['door_state_closed']
        .transform(lambda x: any(~x))       
    )
    
    rawnav_fil_stop_area_5 = rawnav_fil_stop_area_4
    # We start to sort row records into phase based on vars we've created. This is just a first cut.
    rawnav_fil_stop_area_5['rough_phase_by_door'] = np.select(
        [
            (rawnav_fil_stop_area_5.door_state_changes < rawnav_fil_stop_area_5.door_state_changes_min), 
            ((rawnav_fil_stop_area_5.door_state == "O") 
              & (rawnav_fil_stop_area_5.door_state_changes == rawnav_fil_stop_area_5.door_state_changes_min)),
            (rawnav_fil_stop_area_5.door_state_changes > rawnav_fil_stop_area_5.door_state_changes_min),
            (rawnav_fil_stop_area_5.door_state_changes_min.isnull()),
        ], 
        [
            "t_decel_phase", #we'll cut this up a bit further later
            "t_stop1",
            "t_accel_phase", #we'll cut this up a bit further later
            "t_nopax", #we'll apply different criteria to this later        
        ], 
        default="doh" 
    )
    
    # Some buses will stop but not take passengers, so we can't use door openings to cue what 
    # phase the bus is in. in these cases, we'll take the first time the bus hits a full stop 
    # to end the decel phase. We could do this method for all runs (and maybe we should), but for now
    # leaving this as different (i.e. a bus that opens its doors at t_10 may have come to a full
    # stop earlier at t_5 and again at t_10; thus, the phase as calculated by veh state and 
    # by door state can be inconsistent). In practice, we won't use the values in this column except
    # in some special cases where bus is not serving pax.
    rawnav_fil_stop_area_5['rough_phase_by_veh_state'] = np.select(
       [
        (rawnav_fil_stop_area_5.veh_state_changes < rawnav_fil_stop_area_5.veh_stopped_min),
        (rawnav_fil_stop_area_5.veh_state_changes == rawnav_fil_stop_area_5.veh_stopped_min),
        ((rawnav_fil_stop_area_5.veh_state_changes > rawnav_fil_stop_area_5.veh_stopped_min)
         & (rawnav_fil_stop_area_5.veh_state_changes <= rawnav_fil_stop_area_5.veh_stopped_max)),
        (rawnav_fil_stop_area_5.veh_state_changes > rawnav_fil_stop_area_5.veh_stopped_max),
        (rawnav_fil_stop_area_5.veh_stopped_min.isnull())
       ],     
       [
        "t_decel_phase",
        "t_stop", #not tstop1, note - essentially just that the vehicle is stopped
        "t_l_addl",
        "t_accel_phase",
        "t_nostopnopax"
       ],        
       default = 'not relevant'
    )
        
    # In cases where bus is stopped around door open, we do special things.
    # First, we flag rows where bus is literally stopped to pick up passengers.
    # Note that based on t_stop1 definition, this only happens first time bus opens doors
    rawnav_fil_stop_area_5['at_stop']= (
        rawnav_fil_stop_area_5
        .groupby(['filename','index_run_start','stop_id','veh_state_changes'])['rough_phase_by_door']
        .transform(lambda var: var.isin(['t_stop1']).any())
    )

    rawnav_fil_stop_area_5['at_stop_phase'] = np.select(
        [
            ((rawnav_fil_stop_area_5.at_stop) 
             # One might consider condition that is less sensitive. Maybe speed under 2 mph?
             # Note that we don't use a test on fps_next because 0 dist and 0 second ping could
             # lead to NA value
                 & (rawnav_fil_stop_area_5.odom_ft_marg == 0)
                 & (rawnav_fil_stop_area_5.rough_phase_by_door == "t_decel_phase")),
            ((rawnav_fil_stop_area_5.at_stop) 
                & (rawnav_fil_stop_area_5.odom_ft_marg == 0)
                & (rawnav_fil_stop_area_5.rough_phase_by_door == "t_accel_phase"))
        ],
        [
            "t_l_initial",
            "t_l_addl"
        ],
        default = "NA" # NA values aren't problematic here, to be clear
    )

    # Finally, we combine the door state columns for the decomposition
    rawnav_fil_stop_area_6 = (
        rawnav_fil_stop_area_5
        # Assign the at_stop_phase corrections
        .assign(stop_area_phase = lambda x: np.where(x.at_stop_phase != "NA",
                                                     x.at_stop_phase,
                                                     x.rough_phase_by_door))
        # Assign the additional records between the first door closing to last door closing to
        # t_l_addl as well
        .assign(stop_area_phase = lambda x: np.where(
            (x.stop_area_phase == "t_accel_phase")
            & (x.door_state_changes <= x.door_state_changes_max),
            "t_l_addl",
            x.stop_area_phase
            )
        )
    )
    # And we do a final pass cleaning up the runs that don't serve passengers or don't stop at all
    stop_area_decomp = (  
        rawnav_fil_stop_area_6
        # runs that don't stop
        .assign(stop_area_phase = lambda x: np.where(x.any_veh_stopped == False,
                                                     "t_nostopnopax",
                                                     x.stop_area_phase))
        .assign(stop_area_phase = lambda x: np.where(((x.any_door_open == False) 
                                                      & (x.any_veh_stopped == True)),
                                                     x.rough_phase_by_veh_state,
                                                     x.stop_area_phase)
        )
    )
    
    # Note: Columns maintained in output are likely excessive for most needs, but are left in 
    # for any debugging necessary.
    return(stop_area_decomp)

# Helper Functions 
# ################

def filter_to_segment(rawnav,
                      summary):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame
        rawnav data.
    summary: pd.DataFrame
        dataframe summary of runs. 
    Returns
    -------
    rawnav_seg_fil: pd.DataFrame
        rawnav data filtered to points within segments.
    Notes
    -----
    Note that our segment_summary is already filtered to patterns that are in the correct 
    direction for our segments. This join then filters our rawnav data to those relevant 
    runs. 
    Note also that this includes the last value at end_odom_ft_segment (<=), rather than
    being right closed. This improves the ease of certain min/max calculations, but note that
    the last marginal value of speeds or odometer values should be discarded before summing.
    """
    rawnav_seg_fil = (
        rawnav
        .merge(summary[["filename",
                                "index_run_start",
                                "start_odom_ft_segment",
                                "end_odom_ft_segment"]],
               on = ["filename","index_run_start"],
               how = "right")        
        .query('odom_ft >= start_odom_ft_segment & odom_ft <= end_odom_ft_segment')
        .drop(['start_odom_ft_segment','end_odom_ft_segment'], axis = 1)
    )
    
    return(rawnav_seg_fil)
    

def calc_rolling_vals(rawnav,
                      groupvars = ['filename','index_run_start']):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    groupvars: list of column names. 
    Returns
    -------
    rawnav_add: pd.DataFrame, rawnav data with additional fields.
    Notes
    -----
    Because wmatarawnav functions generally leave source rawnav data untouched except 
    just prior to the point of analysis, these calculations may be performed several times
    on smaller chunks of data. We group by stop_id in addition to run in case a segment has 
    multiple stops in it (e.g., Georgia & Irving)
    
    By default calculations are grouped by run, but in certain phases of data processing, it
    can be appropriate to group by run and stop.
    """
    
    rawnav[['odom_ft_next','sec_past_st_next']] = (
        rawnav
        .groupby(groupvars, sort = False)[['odom_ft','sec_past_st']]
        .shift(-1)
    )

    # We'll use a bigger lag for more stable values for free flow speed
    rawnav[['odom_ft_next3','sec_past_st_next3']] = (
        rawnav
        .groupby(groupvars, sort = False)[['odom_ft','sec_past_st']]
        .shift(-3)
    )
    
    rawnav_add = (
        rawnav
        .assign(
            secs_marg=lambda x: x.sec_past_st_next - x.sec_past_st,
            odom_ft_marg=lambda x: x.odom_ft_next - x.odom_ft,
            fps_next=lambda x: ((x.odom_ft_next - x.odom_ft) / 
                                (x.sec_past_st_next - x.sec_past_st)),
            fps_next3=lambda x: ((x.odom_ft_next3 - x.odom_ft) / 
                                 (x.sec_past_st_next3 - x.sec_past_st))
        )
    )
    
    return(rawnav_add)
    
def calc_ad_decomp(nonstop,stop, summary):
    """
    Parameters
    ----------
    nonstop: pd.DataFrame, decomposition of non-stop area
    stop: pd.DataFrame, decomposition of stop area
    summary: pd.DataFrame, segment summary
    Returns
    -------
    ad_method_total: pd.DataFrame, decomposition function
    Notes
    -----
    The 'accerlation-deceleration' decomposition method parent function. 
    
    In practice, this decomposition method remains incomplete and unused, with some calculations 
    remaining in exploratory R notebooks. The code is left-as-is and not deleted in the expectation
    that it may be completed in the future. 
    
    A few issues remain to be addressed:    
    -- Vehicles can appear to be in the 'acceleration' phase for a long time despite
    actually moving at very slow speeds near a stop (i.e. experiencing signal delay). While this
    is also a challenge in the simpler traveltime decomposition method ultimately used, the 
    specificity of this method can result in somewhat misleadingly precise values for phases of the
    decomposition.
    -- Additional challenges remained in ensuring the sum of the in-stop-area and out-of-stop-area 
    values produced consistent totals--the development on this method was paused before all of those
    issues could be addressed. In particular, multi-stop cases and cases where there are two 
    rawnav pings with the same odometer value at the beginning or end of a segment require more
    careful handling
    """

    ad_method_stop_by_run = (
        stop
        .groupby(['filename','index_run_start','seg_name_id'], as_index = False)
        # drop the last record, since we're about to sum the marginal values. 
        .apply(lambda x: x.iloc[:-1])
        .groupby(['filename','index_run_start','seg_name_id','stop_area_phase'])
        .agg({'secs_marg' : ['sum']})
        .pipe(ll.reset_col_names)
        .rename(columns = {'secs_marg_sum':'secs'})
        .pivot_table(
            index = ['filename','index_run_start','seg_name_id'],
            columns = ['stop_area_phase'],
            values = ['secs']
        )
       .pipe(ll.reset_col_names)
    )
        
    # TODO: make sure between is captured
    ad_method_nonstop_by_run = (
        nonstop
        .filter(items = ['filename',
                         'index_run_start',
                         'seg_name_id',
                         'segment_part',
                         'subsegment_min_sec',
                         'subsegment_delay_sec']
        )
        .pivot_table(
            index = ['filename','index_run_start','seg_name_id'],
            values = ['subsegment_min_sec','subsegment_delay_sec'],
            columns = ['segment_part']
        )
        .pipe(ll.reset_col_names)
    )
    
    # Again, add columns for levels in the data were not present. Relative to more careful
    # use of categorical values, this presents fewer problems.
    if ('subsegment_min_sec_between_stop_area' not in ad_method_nonstop_by_run.columns):
        ad_method_nonstop_by_run= ad_method_nonstop_by_run.assign(subsegment_min_sec_between_stop_area = 0,
                                                                  subsegment_delay_sec_between_stop_area = 0)
        
    ad_method_total = (
        ad_method_stop_by_run
        .merge(
            ad_method_nonstop_by_run,
            on = ['filename','index_run_start','seg_name_id'],
            how = 'left'
        )
        .fillna(0)
        .set_index(['filename','index_run_start','seg_name_id'])
        .assign(secs_total = lambda x: x.sum(axis=1))
        .reset_index()
        .merge(
            summary,
            on = ['filename','index_run_start','seg_name_id'],
            how = "left"
            )
        .assign(total_diff = lambda x: x.trip_dur_sec_segment - x.secs_total)
    )

    return(ad_method_total)
