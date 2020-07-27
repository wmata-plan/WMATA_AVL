# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 07:13:54 2020

@author: WylieTimmerman
"""
import pandas as pd
import numpy as np

def decompose_segment_ff(rawnav,
                         segment_summary,
                         max_fps = 73.3):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    segment_summary: pd.DataFrame, defines segment start and end points
    max_fps: np.int or np.float, defines maximum speed above which values will be discarded.
        By default, approx 50 mph (73.3 fps)
    Returns
    -------
    rawnav_add: pd.DataFrame, rawnav data with additional fields.
    Notes
    -----
    Because wmatarawnav functions generally leave source rawnav data untouched except 
    just prior to the point of analysis, these calculations may be performed several times
    on smaller chunks of data.
    """

    rawnav_fil = filter_to_segment(rawnav,
                                   segment_summary)
    
    rawnav_fil = calc_rolling_vals(rawnav)
    
    freeflow_seg = (
        rawnav_fil
        .loc[lambda x: x.fps_next3 < max_fps, 'fps_next3']
        .quantile([0.01, 0.05, 0.10, 0.15, 0.25, 0.5, 0.75, 0.85, 0.90, 0.95, 0.99])
        .to_frame()
        .assign(mph = lambda x: x.fps_next3 / 1.467)
    )
    
    return(freeflow_seg)

def decompose_nonstoparea_ff(rawnav,
                             segment_summary,
                             stop_index_fil,
                             stop_area_upstream_ft = 150,
                             stop_area_downstream_ft = 150,
                             max_fps = 73.3):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    segment_summary: pd.DataFrame, defines segment start and end points
    max_fps: np.int or np.float, defines maximum speed above which values will be discarded.
        By default, approx 50 mph (73.3 fps)
    Returns
    -------
    rawnav_add: pd.DataFrame, rawnav data with additional fields.
    Notes
    -----
    This supports an alternative decomposion

    """

    # Filter to the segment
    rawnav_fil = (
        rawnav
        .merge(segment_summary[["filename",
                                "index_run_start",
                                "start_odom_ft_segment",
                                "end_odom_ft_segment"]],
               on = ["filename","index_run_start"],
               how = "right")
        .query('odom_ft >= start_odom_ft_segment & odom_ft < end_odom_ft_segment')
        .drop(['start_odom_ft_segment','end_odom_ft_segment'], axis = 1)
    )

    rawnav_fil = calc_rolling_vals(rawnav_fil)    
    # filter to portions of segment
    rawnav_nonstoparea = (
        rawnav_fil
        .merge(
            stop_index_fil
            .filter(items = ['filename','index_run_start','odom_ft_qj_stop','stop_id']),
            on = ['filename','index_run_start'],
            how = "left"
        )
        .assign(
            upstream_ft = stop_area_upstream_ft,
            downstream_ft = stop_area_downstream_ft,
            segment_part = lambda x: 
                np.where(x.odom_ft < (x.odom_ft_qj_stop - x.upstream_ft),
                         'before_stop_area',
                         np.where(x.odom_ft >= (x.odom_ft_qj_stop + x.downstream_ft),
                                  'after_stop_area',
                                  'stop_area')
                         )
        )
        .query('(segment_part == "before_stop_area") | (segment_part == "after_stop_area")')
    )
        
        
    nonstoparea_ff_seg = (
        rawnav_nonstoparea
        .loc[lambda x, y = max_fps: x.fps_next3 < y]
        .groupby(['segment_part'])["fps_next3"]
        .quantile([0.01, 0.05, 0.10, 0.15, 0.25, 0.5, 0.75, 0.85, 0.90, 0.95, 0.99])
        .to_frame()
        .reset_index()
        .loc[lambda x: x.level_1 == 0.95]
        .drop(columns = ['level_1'])
        .rename(columns = {'fps_next3' : 'fps_ff'})
    )
    
    
    decomp_nonstoparea = (
        rawnav_nonstoparea
        .groupby(['filename','index_run_start','segment_part'])
        .agg({'odom_ft' : ['min','max'],
              'sec_past_st' : ['min','max']})
    )
    
    decomp_nonstoparea.columns = ["_".join(x) for x in decomp_nonstoparea.columns.ravel()]

    decomp_nonstoparea = (
        decomp_nonstoparea
        .reset_index()
        .assign(subsegment_ft = lambda x: x.odom_ft_max - x.odom_ft_min,
                subsegment_secs = lambda x: x.sec_past_st_max - x.sec_past_st_min)
        .merge(
            nonstoparea_ff_seg,
            on = ['segment_part'],
            how = 'left')
        .assign(subsegment_min_sec = lambda x: x.subsegment_ft / x.fps_ff,
                subsegment_delay_sec = lambda x: x.subsegment_secs - x.subsegment_min_sec)
    )
        
    return(decomp_nonstoparea)


def decompose_stop_area(rawnav,
                        segment_summary,
                        stop_index_fil,
                        stop_area_upstream_ft = 150,
                        stop_area_downstream_ft = 150):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    segment_summary: pd.DataFrame, defines segment start and end points. Should already
        be filtered to the correct patterns relevant to segments
    stop_index_fil: pd.DataFrame, defines index points of stops
    Returns
    -------
    rawnav_fil_stop_area_decomp: pd.DataFrame, rawnav data with additional fields illustrating how 
    each run has been decomposed within the stop area, an input to other analyses.
    Notes
    -----
    rawnav_stop_area
    """

    # TODO: parameter checks

    rawnav_fil_1 = filter_to_segment(rawnav,
                                     segment_summary)

    # We'll also filter to those runs that have a match to the QJ stop while adding detail
    # about the QJ stop zone, just in case any of these runs behaved normally at segment ends
    # but oddly at the stop zone.
    # TODO: rethink this for the multiple stop case, this won't always work
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

    # 3 Add Additional Metrics to Rawnav Data 
    # Downstream calculations will require certain values like the speed over the next interval
    rawnav_fil = calc_rolling_vals(rawnav_fil)
   
    # 5. Filter to Stop Area and Run Stop Area Decomposition
    #############################
    # Create phase identifers, including file, date, and run identifier
        # seg_name_id = segment identifier (ala "sixteenth_u") 
        # stop_zone_id = Stop identifier (likely the stop ID) 
        # t_decel_phase = time from start of segment to zero speed (or door open) (used to estimate adec)
        # t_l_inital = time from first door open back to beginning of zero speed, if any
        # t_stop1 = Stop zone door open time defined as first instance of door opening.
        # t_l_addl = time from first door close to acceleration
        # t_accel_phase = time from acceleration to exiting stop zone (used to help estimate aacc
    # TODO: think harder about how to handle the two-stop case for 70 in georgia/irving
    rawnav_fil_stop_area_1 = (
        rawnav_fil
        .query('odom_ft >= (odom_ft_qj_stop - @stop_area_upstream_ft) & odom_ft < (odom_ft_qj_stop + @stop_area_downstream_ft)')
        .reset_index()
    )
    
    # Add binary variables
    rawnav_fil_stop_area_2 = (
        rawnav_fil_stop_area_1
        .assign(
            door_state_closed=lambda x: x.door_state == "C",
            # note this returns False when fps_next is undefined. 
            veh_state_moving=lambda x: x.fps_next > 0                                                                                               
        )
    )
    
    # Add a sequential numbering that increments each time door changes in a run/segment combination   
    rawnav_fil_stop_area_2['door_state_changes'] = (
        rawnav_fil_stop_area_2
        .groupby(['filename','index_run_start'])['door_state_closed']
        .transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    # We have to be more careful for vehicle state changes. At times, we'll get undefined speeds
    # (e.g., two pings have the same distance and time values) and this could be categorized 
    # as a change in state if we use the same approach as above. Instead, we'll create a separate
    # table without bad speed records, run the calc, join back to the original dataset, and then
    # fill the missing values based on nearby ones
    veh_state = (
        rawnav_fil_stop_area_2
        .filter(items = ['filename','index_run_start','index_loc','veh_state_moving'])
        .loc[~rawnav_fil_stop_area_2.fps_next.isnull()]
    )
    
    veh_state['veh_state_changes'] = (
            veh_state
            .groupby(['filename','index_run_start'])['veh_state_moving']
            .transform(lambda x: x.diff().ne(0).cumsum())
    )
    
    # TODO: update to switch transform to apply throughotu
    rawnav_fil_stop_area_2 = (
        rawnav_fil_stop_area_2
        .merge(
            veh_state
            .drop(columns = ['veh_state_moving']),
            on = ['filename','index_run_start','index_loc'],
            how = 'left'
        )
        # note that this could miss cases of transition where the null value for speed occurs
        # at a stop where passengers board/alight. however, if that's the case, we don't use 
        # these values anyhow.
        .assign(veh_state_changes = lambda x: x.veh_state_changes.ffill())
        .assign(veh_state_changes = lambda x: x.veh_state_changes.bfill())
    )
        
    # To identify the case that's the first door opening at a stop (and the last),
    # we'll summarize to a different dataframe and rejoin. (Grouped mutates don't play well in pandas) 
    # min is almost always 2, but we're extra careful here.  
    # max will be interesting - we'll add anything after the first door closing to the last reclosing
    # as signal delay, essentially.
    door_open_cases = (
        rawnav_fil_stop_area_2
        .loc[rawnav_fil_stop_area_2.door_state == "O"]
        .groupby(['filename','index_run_start','door_state'])
        .agg({"door_state_changes" : ['min','max']})
        .reset_index()
    )
    
    door_open_cases.columns = ["_".join(x) for x in door_open_cases.columns.ravel()]

    door_open_cases = (
        door_open_cases
        .rename(columns = {"filename_" : "filename", "index_run_start_": "index_run_start"})
        .drop(columns = ['door_state_'])
    )
       
    # Before we make use of that new column, we'll do a similar check on where the
    # bus came to be not moving. The language is a little fuzzy here -- we'll call this
    # 'veh_stop' to distinguish that we're talking about the bus literally not moving, 
    # rather than something to do with a 'bus stop'
    veh_stop_cases = (
        rawnav_fil_stop_area_2
        .loc[(~rawnav_fil_stop_area_2.veh_state_moving & rawnav_fil_stop_area_2.fps_next.notnull())]
        .groupby(['filename','index_run_start','veh_state_moving'])
        .agg({"veh_state_changes" : ['min','max']})
        .reset_index()
    )
    
    veh_stop_cases.columns = ["_".join(x) for x in veh_stop_cases.columns.ravel()]
    
    veh_stop_cases = (
        veh_stop_cases 
        .rename(columns = {"filename_" : "filename",
                          "index_run_start_": "index_run_start",
                          "veh_state_changes_min": "veh_stopped_min",
                          "veh_state_changes_max": "veh_stopped_max"})
        .drop(columns = ['veh_state_moving_'])
    )
     
    rawnav_fil_stop_area_3 = (
        rawnav_fil_stop_area_2
        .merge(
            door_open_cases,
            on = ['filename','index_run_start'],
            how = 'left'
        )
        .merge(
            veh_stop_cases,
            on = ['filename','index_run_start'],
            how = 'left'
        )
    )
    
    # For convience in other downstream calcs, 
    # we'll add flags to help with certain cases where vehicle didn't stop at all or doors didn't open
    # We'll reuse a table we made earlier since we've dealt with fps_next nulls here
    veh_state_any_move = (
        veh_state 
        .drop(columns = ['veh_state_changes'])
    )
    
    veh_state_any_move['any_veh_stopped'] = (
        veh_state_any_move
        .groupby(['filename','index_run_start'])['veh_state_moving']
        .transform(lambda x: any(~x))
    )
    
    rawnav_fil_stop_area_3 = (
        rawnav_fil_stop_area_3 
        .merge(
            veh_state_any_move
            .drop(columns = ['veh_state_moving']),
            on = ['filename','index_run_start','index_loc'],
            how = "left")
        # some NA's may appear after join. We'll usually fill to address these,
        # but in the case where we know doors are open, we'll override
        # we still run into issues with this run
        # rawnav03220191021.txt, 5124  so we may want to fix it
        .assign(any_veh_stopped = lambda x: np.where(x.any_veh_stopped.isnull() 
                                                      & x.door_state == "O",
                                                     True,
                                                     x.any_veh_stopped)
        )
        .assign(any_veh_stopped = lambda x: x.any_veh_stopped.ffill())
        .assign(any_veh_stopped = lambda x: x.any_veh_stopped.bfill())
    )
        
    # Similar approach for door open but directly applied
    rawnav_fil_stop_area_3['any_door_open'] = (
        rawnav_fil_stop_area_3
        .groupby(['filename','index_run_start'])['door_state_closed']
        .transform(lambda x: any(~x))       
    )
   
    # We start to sort row records based on vars we've created. This is just a first cut.
    # this is casewhen, if you're wondering. "rough_phase" is our first cut at the basic 
    # bus state decomposition,
    rawnav_fil_stop_area_3['rough_phase_by_door'] = np.select(
        [
            (rawnav_fil_stop_area_3.door_state_changes < rawnav_fil_stop_area_3.door_state_changes_min), #
            ((rawnav_fil_stop_area_3.door_state == "O") 
              & (rawnav_fil_stop_area_3.door_state_changes == rawnav_fil_stop_area_3.door_state_changes_min)),
            (rawnav_fil_stop_area_3.door_state_changes > rawnav_fil_stop_area_3.door_state_changes_min),
            (rawnav_fil_stop_area_3.door_state_changes_min.isnull()),
        ], 
        [
            "t_decel_phase", #we'll cut this up a bit further later
            "t_stop1",
            "t_accel_phase", #we'll cut this up a bit further later
            "t_nopax", #we'll apply different criteria to this later #          
        ], 
        default="doh" 
    )
    
    # Some buses will stop but not take passengers, so we can't use door openings to cue what 
    # phase the bus is in. in these cases, we'll take the first time the bus hits a full stop 
    # to end the decel phase. WE could do that for all trips (and maybe we should), but for now
    # leaving this as different. In practice, we won't 'use' the values in this column except
    # in some special cases
    rawnav_fil_stop_area_3['rough_phase_by_veh_state'] = np.select(
       [
        (rawnav_fil_stop_area_3.veh_state_changes < rawnav_fil_stop_area_3.veh_stopped_min),
        (rawnav_fil_stop_area_3.veh_state_changes == rawnav_fil_stop_area_3.veh_stopped_min),
        ((rawnav_fil_stop_area_3.veh_state_changes > rawnav_fil_stop_area_3.veh_stopped_min)
         & (rawnav_fil_stop_area_3.veh_state_changes <= rawnav_fil_stop_area_3.veh_stopped_max)),
        (rawnav_fil_stop_area_3.veh_state_changes > rawnav_fil_stop_area_3.veh_stopped_max),
        (rawnav_fil_stop_area_3.veh_stopped_min.isnull())
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
        
    # in cases where bus is stopped around door open, we do special things
    # first, we flag rows where bus is literally stopped to pick up passengers
    # note that based on t_stop1 definition, this only happens first time bus opens doors
    rawnav_fil_stop_area_3['at_stop']= (
        rawnav_fil_stop_area_3
        .groupby(['filename','index_run_start','veh_state_changes'])['rough_phase_by_door']
        .transform(lambda var: var.isin(['t_stop1']).any())
    )

    rawnav_fil_stop_area_3['at_stop_phase'] = np.select(
        [
            ((rawnav_fil_stop_area_3.at_stop) 
             # TODO: consider condition that is less sensitive. Maybe speed under 2 mph?
             # Note that we don't use a test on fps_next because 0 dist and 0 second ping could
             # lead to NA value
                 & (rawnav_fil_stop_area_3.odom_ft_marg == 0)
                 & (rawnav_fil_stop_area_3.rough_phase_by_door == "t_decel_phase")),
            ((rawnav_fil_stop_area_3.at_stop) 
                & (rawnav_fil_stop_area_3.odom_ft_marg == 0)
                & (rawnav_fil_stop_area_3.rough_phase_by_door == "t_accel_phase"))
        ],
        [
            "t_l_initial",
            "t_l_addl"
        ],
        default = "NA" # NA values aren't problematic here, to be clear
    )

    # Finally, we combine the door state columns for the decomposition
    rawnav_fil_stop_area_4 = (
        rawnav_fil_stop_area_3
        # assign the at_stop_phase corrections
        .assign(stop_area_phase = lambda x: np.where(x.at_stop_phase != "NA",
                                                     x.at_stop_phase,
                                                     x.rough_phase_by_door))
        # assign the additional records between the first door closing to last door closing to
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
    rawnav_fil_stop_area_decomp = (  
        rawnav_fil_stop_area_4
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
    
    #TODO: Consider what columns used to support calculations to retain in the output
    return(rawnav_fil_stop_area_decomp)

# Helper Functions 
# ======================================

def filter_to_segment(rawnav,
                      summary):
    
    # Note that our segment_summary is already filtered to patterns that are in the correct 
    # direction for our segments. This join then filters our rawnav data to those relevant 
    # runs.
    rawnav_seg_fil = (
        rawnav
        .merge(summary[["filename",
                                "index_run_start",
                                "start_odom_ft_segment",
                                "end_odom_ft_segment"]],
               on = ["filename","index_run_start"],
               how = "right")        
        .query('odom_ft >= start_odom_ft_segment & odom_ft < end_odom_ft_segment')
        .drop(['start_odom_ft_segment','end_odom_ft_segment'], axis = 1)
    )
    
    return(rawnav_seg_fil)
    

def calc_rolling_vals(rawnav):
    """
    Parameters
    ----------
    rawnav: pd.DataFrame, rawnav data. Expect cols sec_past_st and odom_ft
    Returns
    -------
    rawnav_add: pd.DataFrame, rawnav data with additional fields.
    Notes
    -----
    Because wmatarawnav functions generally leave source rawnav data untouched except 
    just prior to the point of analysis, these calculations may be performed several times
    on smaller chunks of data.
    """
    
    rawnav[['odom_ft_next','sec_past_st_next']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[['odom_ft','sec_past_st']]
        .shift(-1)
    )

    # We'll use a bigger lag for more stable values for free flow speed
    rawnav[['odom_ft_next3','sec_past_st_next3']] = (
        rawnav
        .groupby(['filename','index_run_start'], sort = False)[['odom_ft','sec_past_st']]
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
    
    