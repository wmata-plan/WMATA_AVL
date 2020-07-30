# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 03:39:56 2020

@author: WylieTimmerman, Apoorba Bibeka

Note: the sections here are guideposts - this may be better as several scripts
"""

# 0 Housekeeping. Clear variable space
####################################################################################################
from IPython import get_ipython

ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")

# 1 Import Libraries and Set Global Parameters
####################################################################################################
# 1.1 Import Python Libraries
############################################
from datetime import datetime

print("Run Section 1 Import Libraries and Set Global Parameters...")
begin_time = datetime.now()

import os, sys, shutil

if not sys.warnoptions:
    import warnings
    # warnings.simplefilter("ignore")  # Stop Pandas warnings

import os, sys
import numpy as np
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

# 1.2 Set Global Parameters
############################################

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\Users\WylieTimmerman\Documents\projects_local\wmata_avl_local"
    path_source_data = os.path.join(path_sp, "data", "00-raw")
    path_processed_data = os.path.join(path_sp, "data", "02-processed")
    path_segments = path_processed_data

elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents" \
                       r"\WMATA-AVL\Data\field_rawnav_dat"
    gtfs_dir = os.path.join(path_source_data, "google_transit")
    # Processed data
    path_processed_data = os.path.join(path_source_data, "processed_data")
    path_wmata_schedule_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data" \
                           r"\wmata_schedule_data\Schedule_082719-201718.mdb"
    path_segments = path_processed_data
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")


# Globals
# Queue Jump Routes
q_jump_route_list = ['S1', 'S2', 'S4', 'S9',
                     '70', '79',
                     '64', 'G8',
                     'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
analysis_routes = q_jump_route_list
# EPSG code for WMATA-area work
wmata_crs = 2248

# maximum feet per second allowable for freeflow speeds (50 mph ~ 73.3 fps)
max_fps = 73.3

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

# 1.4 Reload Relevant Files
############################################
# TODO: Should we move this to a project specific folder or something? Right now I'm just recopying it
# 2.1. Load segment-pattern-stop crosswalk
# This crosswalk is used to connect segment shapes to rawnav data. The 'route' field must
# match the same string used in the rawnav data. 'direction' will be looked up in a moment
# against the wmata schedule database
# and replaced with a pattern code as an int32 value. 'seg_name_id' is also found in the segment
# geometry file. 'stop_id' matches the stop identifier in the WMATA schedule database.
xwalk_seg_pattern_stop_in = wr.tribble(
    ['route', 'direction', 'seg_name_id', 'stop_id'],
    "79", "SOUTH", "georgia_columbia", 10981,
    "79", "SOUTH", "georgia_piney_branch_long", 4217,
    # not sure yet how to deal with second stop, but i think this works
    "70", "SOUTH", "georgia_irving", 19186,  # irving stop
    "70", "SOUTH", "georgia_irving", 10981,  # columbia stop
    "70", "SOUTH", "georgia_piney_branch_shrt", 4217,
    "S1", "NORTH", "sixteenth_u_shrt", 18042,
    "S2", "NORTH", "sixteenth_u_shrt", 18042,
    "S4", "NORTH", "sixteenth_u_shrt", 18042,
    "S9", "NORTH", "sixteenth_u_long", 18042,
    "64", "NORTH", "eleventh_i_new_york", 16490,
    "G8", "EAST", "eleventh_i_new_york", 16490,
    "D32", "EAST", "irving_fifteenth_sixteenth", 2368,
    "H1", "NORTH", "irving_fifteenth_sixteenth", 2368,
    "H2", "EAST", "irving_fifteenth_sixteenth", 2368,
    "H3", "EAST", "irving_fifteenth_sixteenth", 2368,
    "H4", "EAST", "irving_fifteenth_sixteenth", 2368,
    "H8", "EAST", "irving_fifteenth_sixteenth", 2368,
    "W47", "EAST", "irving_fifteenth_sixteenth", 2368
    # "64",            "NORTH",                    "nh_3rd_test",   17329, #4th street
    # "64",            "NORTH",                    "nh_3rd_test",   25370, #3rd street
    # "G8",             "EAST",                 "ri_lincoln_test",  26282
)


xwalk_wmata_route_dir_pattern = (
    wr.read_sched_db_patterns(
        path=path_wmata_schedule_data,
        analysis_routes=analysis_routes)
        .filter(items=['direction', 'route', 'pattern'])
        .drop_duplicates()
)

xwalk_seg_pattern_stop = (
    xwalk_seg_pattern_stop_in
        .merge(xwalk_wmata_route_dir_pattern, on=['route', 'direction'])
        .drop('direction', 1)
        .reindex(columns=['route', 'pattern', 'seg_name_id', 'stop_id'])
        .query("""seg_name_id in ['georgia_columbia','georgia_irving','sixteenth_u_shrt']""")
)

del xwalk_seg_pattern_stop_in

# 3. load shapes
# Segments
# Note unique identifier seg_name_id
# Note that these are not yet updated to reflect the extension of the 11th street segment
# further south to give the stop more breathing room.
segments = (
    gpd.read_file(os.path.join(path_segments, "segments.geojson"))
        .to_crs(wmata_crs)
)

# 3 Filter Out Runs that Appear Problematic
###########################################\
freeflow_list = []

# Set up folder to dump results to
# TODO: improve path / save behavior
path_stop_area_dump = os.path.join(path_processed_data, "rawnav_stop_areas")
if not os.path.isdir(path_stop_area_dump):
    os.mkdir(path_stop_area_dump)

for seg in list(
        xwalk_seg_pattern_stop.seg_name_id.drop_duplicates()):  # ["eleventh_i_new_york"]: #list(xwalk_seg_pattern_stop.seg_name_id.drop_duplicates()):
    print('now on {}'.format(seg))
    # 1. Read-in Data
    #############################
    # Reduce rawnav data to runs present in the summary file after filtering.
    # Note that this file may have also been the product of a filtered rawnav_summary table
    # One doesn't want to do a straight merge -- that has issues in that some runs will appear twice in the
    # segment summary file because they have multiple segments defined. This will keep them if they
    # meet criteria for any of their segments and will not duplicate. #TODO : test this.
    # Ideally we could shortcut this using the index, but the index for this table is a little different
    # TODO: could use itertuples but i find that syntax really weird, frankly. SHould we change?

    xwalk_seg_pattern_stop_fil = xwalk_seg_pattern_stop.query('seg_name_id == @seg')

    seg_routes = list(xwalk_seg_pattern_stop_fil.route.drop_duplicates())

    rawnav_dat = (
        wr.read_cleaned_rawnav(
            analysis_routes_=seg_routes,
            path=os.path.join(path_processed_data, "rawnav_data.parquet"))
            .drop(columns=['blank', 'lat_raw', 'long_raw', 'sat_cnt'])
    )

    segment_summary = (
        pq.read_table(source=os.path.join(path_processed_data, "segment_summary.parquet"),
                      filters=[['seg_name_id', "=", seg]],
                      use_pandas_metadata=True)
            .to_pandas()
    )

    # TODO: fix the segment code so this is unneccessary -- not sure why this is doing this now
    segment_summary = segment_summary[~segment_summary.duplicated(['filename', 'index_run_start'], keep='last')]

    stop_index = (
        pq.read_table(source=os.path.join(path_processed_data, "stop_index.parquet"),
                      filters=[[('route', '=', route)] for route in seg_routes],
                      columns=['seg_name_id',
                               'route',
                               'pattern',
                               'stop_id',
                               'filename',
                               'index_run_start',
                               'index_loc',
                               'odom_ft',
                               'sec_past_st',
                               'geo_description'],
                      use_pandas_metadata=True)
            .to_pandas()
            .assign(pattern=lambda x: x.pattern.astype('int32'))  # pattern is object not int? # TODO: fix
            .rename(columns={'odom_ft': 'odom_ft_qj_stop'})
    )

    # NOTE: this would be the place to filter out runs where a high number of stops don't match
    # but we're okay with runs that we don't have complete data on
    # Filter Stop index to the relevant QJ stops
    stop_index_fil = (
        stop_index
            .merge(xwalk_seg_pattern_stop_fil,
                   on=['route', 'pattern', 'stop_id'],
                   how='inner')
    )

    # 2 Join and Filter to Segment
    #############################
    # Note that our segment_summary is already filtered to patterns that are in the correct
    # direction for our segments. This join then filters our rawnav data to those relevant
    # runs.

    rawnav_fil_1 = (
        rawnav_dat
            .merge(segment_summary[["filename",
                                    "index_run_start",
                                    "start_odom_ft_segment",
                                    "end_odom_ft_segment"]],
                   on=["filename", "index_run_start"],
                   how="right")
            .query('odom_ft >= start_odom_ft_segment & odom_ft < end_odom_ft_segment')
            .drop(['start_odom_ft_segment', 'end_odom_ft_segment'], axis=1)
    )

    del rawnav_dat

    # We'll also filter to those runs that have a match to the QJ stop while adding detail
    # about the QJ stop zone, just in case any of these runs behaved normally at segment ends
    # but oddly at the stop zone.
    # TODO: rethink this for the multiple stop case, this won't always work
    # AxB: I am hard coding for now to only keep columbia
    if seg == "georgia_irving":
        stop_index_fil = stop_index_fil.query("stop_id ==10981") # i am removing irving from the georgia_irving segment
    rawnav_fil = (
        rawnav_fil_1
            .merge(
            stop_index_fil
                .filter(items=['filename', 'index_run_start', 'odom_ft_qj_stop', 'stop_id']),
            on=['filename', 'index_run_start'],
            how="left"
        )
    )

    del rawnav_fil_1

    # 3 Add Additional Metrics to Rawnav Data
    #############################
    rawnav_fil[['odom_ft_next', 'sec_past_st_next']] = (
        rawnav_fil
            .groupby(['filename', 'index_run_start'], sort=False)[['odom_ft', 'sec_past_st']]
            .shift(-1)
    )

    # We'll use a bigger lag for more stable values for free flow speed
    rawnav_fil[['odom_ft_next3', 'sec_past_st_next3']] = (
        rawnav_fil
            .groupby(['filename', 'index_run_start'], sort=False)[['odom_ft', 'sec_past_st']]
            .shift(-3)
    )

    rawnav_fil = (
        rawnav_fil
            .assign(
            secs_marg=lambda x: x.sec_past_st_next - x.sec_past_st,
            odom_ft_marg=lambda x: x.odom_ft_next - x.odom_ft,
            fps_next=lambda x: ((x.odom_ft_next - x.odom_ft) /
                                (x.sec_past_st_next - x.sec_past_st)),
            fps_next3=lambda x: ((x.odom_ft_next3 - x.odom_ft) /
                                 (x.sec_past_st_next3 - x.sec_past_st))
        )
    )
    # 4. Free Flow Calcs
    #############################

    freeflow_seg = (
        rawnav_fil
            .loc[lambda x: x.fps_next3 < max_fps, 'fps_next3']
            .quantile([0.01, 0.05, 0.10, 0.15, 0.25, 0.5, 0.75, 0.85, 0.90, 0.95, 0.99])
            .to_frame()
            .assign(mph=lambda x: x.fps_next3 / 1.467,
                    seg_name_id=seg)
    )

    freeflow_list.append(freeflow_seg)

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
            .query('odom_ft >= (odom_ft_qj_stop - 150) & odom_ft < (odom_ft_qj_stop + 150)')
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
            .groupby(['filename', 'index_run_start'])['door_state_closed']
            .transform(lambda x: x.diff().ne(0).cumsum())
    )

    # We have to be more careful for vehicle state changes. At times, we'll get undefined speeds
    # (e.g., two pings have the same distance and time values) and this could be categorized
    # as a change in state if we use the same approach as above. Instead, we'll create a separate
    # table without bad speed records, run the calc, join back to the original dataset, and then
    # fill the missing values based on nearby ones
    veh_state = (
        rawnav_fil_stop_area_2
            .filter(items=['filename', 'index_run_start', 'index_loc', 'veh_state_moving'])
            .loc[~rawnav_fil_stop_area_2.fps_next.isnull()]
    )

    veh_state['veh_state_changes'] = (
        veh_state
            .groupby(['filename', 'index_run_start'])['veh_state_moving']
            .transform(lambda x: x.diff().ne(0).cumsum())
    )

    # TODO: update to switch transform to apply throughotu
    rawnav_fil_stop_area_2 = (
        rawnav_fil_stop_area_2
            .merge(
            veh_state
                .drop(columns=['veh_state_moving']),
            on=['filename', 'index_run_start', 'index_loc'],
            how='left'
        )
            # note that this could miss cases of transition where the null value for speed occurs
            # at a stop where passengers board/alight. however, if that's the case, we don't use
            # these values anyhow.
            .assign(veh_state_changes=lambda x: x.veh_state_changes.ffill())
            .assign(veh_state_changes=lambda x: x.veh_state_changes.bfill())
    )

    # To identify the case that's the first door opening at a stop (and the last),
    # we'll summarize to a different dataframe and rejoin. (Grouped mutates don't play well in pandas)
    # min is almost always 2, but we're extra careful here.
    # max will be interesting - we'll add anything after the first door closing to the last reclosing
    # as signal delay, essentially.
    door_open_cases = (
        rawnav_fil_stop_area_2
            .loc[rawnav_fil_stop_area_2.door_state == "O"]
            .groupby(['filename', 'index_run_start', 'door_state'])
            .agg({"door_state_changes": ['min', 'max']})
            .reset_index()
    )

    door_open_cases.columns = ["_".join(x) for x in door_open_cases.columns.ravel()]

    door_open_cases = (
        door_open_cases
            .rename(columns={"filename_": "filename", "index_run_start_": "index_run_start"})
            .drop(columns=['door_state_'])
    )

    # Before we make use of that new column, we'll do a similar check on where the
    # bus came to be not moving. The language is a little fuzzy here -- we'll call this
    # 'veh_stop' to distinguish that we're talking about the bus literally not moving,
    # rather than something to do with a 'bus stop'
    veh_stop_cases = (
        rawnav_fil_stop_area_2
            .loc[(~rawnav_fil_stop_area_2.veh_state_moving & rawnav_fil_stop_area_2.fps_next.notnull())]
            .groupby(['filename', 'index_run_start', 'veh_state_moving'])
            .agg({"veh_state_changes": ['min', 'max']})
            .reset_index()
    )

    veh_stop_cases.columns = ["_".join(x) for x in veh_stop_cases.columns.ravel()]

    veh_stop_cases = (
        veh_stop_cases
            .rename(columns={"filename_": "filename",
                             "index_run_start_": "index_run_start",
                             "veh_state_changes_min": "veh_stopped_min",
                             "veh_state_changes_max": "veh_stopped_max"})
            .drop(columns=['veh_state_moving_'])
    )

    rawnav_fil_stop_area_3 = (
        rawnav_fil_stop_area_2
            .merge(
            door_open_cases,
            on=['filename', 'index_run_start'],
            how='left'
        )
            .merge(
            veh_stop_cases,
            on=['filename', 'index_run_start'],
            how='left'
        )
    )

    # For convience in other downstream calcs,
    # we'll add flags to help with certain cases where vehicle didn't stop at all or doors didn't open
    # We'll reuse a table we made earlier since we've dealt with fps_next nulls here
    veh_state_any_move = (
        veh_state
            .drop(columns=['veh_state_changes'])
    )

    veh_state_any_move['any_veh_stopped'] = (
        veh_state_any_move
            .groupby(['filename', 'index_run_start'])['veh_state_moving']
            .transform(lambda x: any(~x))
    )

    rawnav_fil_stop_area_3 = (
        rawnav_fil_stop_area_3
            .merge(
            veh_state_any_move
                .drop(columns=['veh_state_moving']),
            on=['filename', 'index_run_start', 'index_loc'],
            how="left")
            # some NA's may appear after join. We'll usually fill to address these,
            # but in the case where we know doors are open, we'll override
            # we still run into issues with this run
            # rawnav03220191021.txt, 5124  so we may want to fix it
            .assign(any_veh_stopped=lambda x: np.where(x.any_veh_stopped.isnull()
                                                       & x.door_state == "O",
                                                       True,
                                                       x.any_veh_stopped)
                    )
            .assign(any_veh_stopped=lambda x: x.any_veh_stopped.ffill())
            .assign(any_veh_stopped=lambda x: x.any_veh_stopped.bfill())
    )

    # Similar approach for door open but directly applied
    rawnav_fil_stop_area_3['any_door_open'] = (
        rawnav_fil_stop_area_3
            .groupby(['filename', 'index_run_start'])['door_state_closed']
            .transform(lambda x: any(~x))
    )

    # We start to sort row records based on vars we've created. This is just a first cut.
    # this is casewhen, if you're wondering. "rough_phase" is our first cut at the basic
    # bus state decomposition,
    rawnav_fil_stop_area_3['rough_phase_by_door'] = np.select(
        [
            (rawnav_fil_stop_area_3.door_state_changes < rawnav_fil_stop_area_3.door_state_changes_min),  #
            ((rawnav_fil_stop_area_3.door_state == "O")
             & (rawnav_fil_stop_area_3.door_state_changes == rawnav_fil_stop_area_3.door_state_changes_min)),
            (rawnav_fil_stop_area_3.door_state_changes > rawnav_fil_stop_area_3.door_state_changes_min),
            (rawnav_fil_stop_area_3.door_state_changes_min.isnull()),
        ],
        [
            "t_decel_phase",  # we'll cut this up a bit further later
            "t_stop1",
            "t_accel_phase",  # we'll cut this up a bit further later
            "t_nopax",  # we'll apply different criteria to this later #            "t_nostopnopax"
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
            "t_stop",  # not tstop1, note - essentially just that the vehicle is stopped
            "t_l_addl",
            "t_accel_phase",
            "t_nostopnopax"
        ],
        default='not relevant'
    )

    # in cases where bus is stopped around door open, we do special things
    # first, we flag rows where bus is literally stopped to pick up passengers
    # note that based on t_stop1 definition, this only happens first time bus opens doors
    rawnav_fil_stop_area_3['at_stop'] = (
        rawnav_fil_stop_area_3
            .groupby(['filename', 'index_run_start', 'veh_state_changes'])['rough_phase_by_door']
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
        default="NA"  # NA values aren't problematic here, to be clear
    )

    # Finally, we combine the door state columns for the decomposition
    rawnav_fil_stop_area_4 = (
        rawnav_fil_stop_area_3
            # assign the at_stop_phase corrections
            .assign(stop_area_phase=lambda x: np.where(x.at_stop_phase != "NA",
                                                       x.at_stop_phase,
                                                       x.rough_phase_by_door))
            # assign the additional records between the first door closing to last door closing to
            # t_l_addl as well
            .assign(stop_area_phase=lambda x: np.where(
            (x.stop_area_phase == "t_accel_phase")
            & (x.door_state_changes <= x.door_state_changes_max),
            "t_l_addl",
            x.stop_area_phase
        )
                    )
    )
    # And we do a final pass cleaning up the runs that don't serve passengers or don't stop at all
    rawnav_fil_stop_area_5 = (
        rawnav_fil_stop_area_4
            # runs that don't stop
            .assign(stop_area_phase=lambda x: np.where(x.any_veh_stopped == False,
                                                       "t_nostopnopax",
                                                       x.stop_area_phase))
            .assign(stop_area_phase=lambda x: np.where(((x.any_door_open == False)
                                                        & (x.any_veh_stopped == True)),
                                                       x.rough_phase_by_veh_state,
                                                       x.stop_area_phase)
                    )
    )
    rawnav_fil_stop_area_5.loc[:,'seg_name_id'] = seg
    rawnav_fil_stop_area_5.to_csv(os.path.join(path_stop_area_dump, "ourpoints_{}.csv".format(seg)))

freeflow = (
    pd.concat(freeflow_list)
        .rename_axis('ntile')
        .reset_index()
)

# Quick dump of values
# TODO: improve path / save behavior
freeflow.to_csv(os.path.join(path_stop_area_dump, "freeflow.csv"))

freeflow_vals = freeflow.query('ntile == 0.95')

# basic_decomp.to_csv(os.path.join(path_stop_area_dump,"basic_decomp.csv"))


# Calculate Stop-level Baseline Accel-Decel Time (input to t_stop2)
####################################################################################################
# TODO : Update accordingly following more discussion with Burak.

# Would require additional filtering of runs meeting or not meeting certain criteria.

# wr.calc_stop_accel_decel_baseline()

# Calculate t_stop2
####################################################################################################

# TODO: write the wr.calc_tstop2s() function


# 3 Travel Time decomposition
####################################################################################################

# Here, we would call a wr.decompose_traveltime() function. I imagine this would take as input

# TODO: write the wr.decompose_traveltime() function


