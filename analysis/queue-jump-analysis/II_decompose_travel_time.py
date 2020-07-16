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
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq

# 1.2 Set Global Parameters
############################################

if os.getlogin() == "WylieTimmerman":
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\Users\WylieTimmerman\Documents\projects_local\wmata_avl_local"
    path_source_data = os.path.join(path_sp,"data","00-raw")
    path_processed_data = os.path.join(path_sp, "data","02-processed")
elif os.getlogin() == "abibeka":
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
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
             ['route',        'direction',                    'seg_name_id','stop_id'], 
                 "79",            "SOUTH",               "georgia_columbia",    '5381',      
                 "79",            "SOUTH",      "georgia_piney_branch_long",    '5401',
                 #not sure yet how to deal with second stop, but i think this works
                 "70",            "SOUTH",                 "georgia_irving",    '5600',
                 "70",            "SOUTH",                 "georgia_irving",    '5381', 
                 "70",            "SOUTH",      "georgia_piney_branch_shrt",    '5401',
                 "S1",            "NORTH",               "sixteenth_u_shrt",    '7848',
                 "S2",            "NORTH",               "sixteenth_u_shrt",    '7848',
                 "S4",            "NORTH",               "sixteenth_u_shrt",    '7848',
                 "S9",            "NORTH",               "sixteenth_u_long",    '7848',
                 "64",            "NORTH",            "eleventh_i_new_york",    '7627',
                 "G8",             "EAST",            "eleventh_i_new_york",    '7627',
                "D32",             "EAST",     "irving_fifteenth_sixteenth",    '7794',
                 "H1",            "NORTH",     "irving_fifteenth_sixteenth",    '7794',
                 "H2",             "EAST",     "irving_fifteenth_sixteenth",    '7794',
                 "H3",             "EAST",     "irving_fifteenth_sixteenth",    '7794',
                 "H4",             "EAST",     "irving_fifteenth_sixteenth",    '7794',
                 "H8",             "EAST",     "irving_fifteenth_sixteenth",    '7794',
                "W47",             "EAST",     "irving_fifteenth_sixteenth",    '7794'
  )

xwalk_wmata_route_dir_pattern = (
    wr.read_sched_db_patterns(
        path = os.path.join(path_source_data,
                            "wmata_schedule_data",
                            "Schedule_082719-201718.mdb"),
            analysis_routes = analysis_routes)
    .filter(items = ['direction','route','pattern'])
    .drop_duplicates()
)
    
xwalk_seg_pattern_stop = (
    xwalk_seg_pattern_stop_in
    .merge(xwalk_wmata_route_dir_pattern, on = ['route','direction'])
    .drop('direction', 1)
    .reindex(columns = ['route','pattern','seg_name_id','stop_id'])
)

del xwalk_seg_pattern_stop_in

# 2. create segment-pattern crosswalk 
xwalk_seg_pattern = (xwalk_seg_pattern_stop.drop('stop_id', 1)
                     .drop_duplicates())

# 3. load shapes
# Segments
# Note unique identifier seg_name_id
# Note that these are not yet updated to reflect the extension of the 11th street segment 
# further south to give the stop more breathing room.
segments = gpd.read_file(
    os.path.join(path_processed_data,"segments.geojson")).\
    to_crs(wmata_crs)
    
segment_summary =\
    pq.read_table(source = os.path.join(path_processed_data,"segment_summary.parquet"),
                  use_pandas_metadata = True,
                  ).to_pandas()
# 3 Filter Out Runs that Appear Problematic
###########################################\
freeflow_list = []

for seg in list(xwalk_seg_pattern.seg_name_id.drop_duplicates()):
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
    
    xwalk_seg_pattern_fil = xwalk_seg_pattern.query('seg_name_id == @seg')
    
    seg_routes = list(xwalk_seg_pattern_fil.route.drop_duplicates())
    
    rawnav_dat = (
        wr.read_cleaned_rawnav(
           analysis_routes_ = seg_routes,
           
           path = os.path.join(path_processed_data, "rawnav_data.parquet"))
        .drop(columns=['blank', 'lat_raw', 'long_raw', 'sat_cnt'])
    )
            
    segment_summary = (
        pq.read_table(source = os.path.join(path_processed_data,"segment_summary.parquet"),
                      filters = [['seg_name_id', "=", seg]],
                      use_pandas_metadata = True)
        .to_pandas()
    )
    # Note that our segment_summary is already filtered to patterns that are in the correct 
    # direction for our segments.
        
    rawnav_fil = (
        rawnav_dat.merge(segment_summary[["filename",
                                                    "index_run_start",
                                                    "start_odom_ft_segment",
                                                    "end_odom_ft_segment"]],
                                  on = ["filename","index_run_start"],
                                  how = "right")        
        .query('odom_ft >= start_odom_ft_segment & odom_ft < end_odom_ft_segment')
        .drop(['start_odom_ft_segment','end_odom_ft_segment'], axis = 1)
    )
    
    del rawnav_dat   

    # 2 Add Additional Metrics to Rawnav Data 
    #############################
    
    # We'll use a bigger lag for more stable values for free flow speed
    rawnav_fil[['odom_ft_next3','sec_past_st_next3']] = (
        rawnav_fil
        .groupby(['filename','index_run_start'], sort = False)\
        [['odom_ft','sec_past_st']]
        .shift(-3)
    )
    
    rawnav_fil = (
        rawnav_fil
        .assign(fps_next3 = lambda x: ((x.odom_ft_next3 - x.odom_ft) / 
                                       (x.sec_past_st_next3 - x.sec_past_st)))
    )
    # 3. Free Flow Calcs 
    #############################

    freeflow_seg = (
        rawnav_fil
        .loc[lambda x: x.fps_next3 < max_fps, 'fps_next3']
        .quantile([0.01, 0.05, 0.10, 0.15, 0.25, 0.5, 0.75, 0.85, 0.90, 0.95, 0.99])
        .to_frame()
        .assign(mph = lambda x: x.fps_next3 / 1.467,
                seg_name_id = seg)
    )

    freeflow_list.append(freeflow_seg)

freeflow = (
    pd.concat(freeflow_list)
    .rename_axis('ntile')
    .reset_index()
)

freeflow.to_csv(os.path.join(path_processed_data,"freeflow.csv"))

freeflow_vals = freeflow.query('ntile == 0.95')

# 4. Do Basic Decomposition of Travel Time by Run
####################################################################################################
# Goal here is to tease out values that we can use to run the currently proposed t_stop2 process
#   or other possible decompositions that may arise later.

# Some complex logic here to implement in a function wr.calc_basic_decomp(). Idea is that we do
#   a basic decomposition here that will be used to extract t_decel_phase and t_accel_phase for 
#   use in the approach outlined by Burak to create a a_acc and a_dec in the methodology workshop
#   Powerpoint. But regardless of how we calculate that, these items are useful. 

# Example result table shown in Powerpoint. Field names can vary to match what we already have! 
#   run_basic_decomp = ...
# Run identifers, including file, date, and run identifier
        # seg_name_id = segment identifier (ala "sixteenth_u") 
        # stop_zone_id = Stop identifier (likely the stop ID) 
        # t_decel_phase = time from start of segment to zero speed (or door open) (used to estimate adec)
        # t_l_inital = time from first door open back to beginning of zero speed, if any
        # t_stop1 = Stop zone door open time defined as first instance of door opening.
        # t_l_addl = time from first door close to acceleration
        # t_accel_phase = time from acceleration to exiting stop zone (used to help estimate aacc
        # t_sz_total = total time in stop zone

# Function inputs include:
    #   - A rawnav file (pulled from Parquet storage, may include many runs and even multiple routes or segments)
    #   - The index_stop_zone_start_end table with one record for the start or stop of each stop zone -- rawnav run -- evaluation segment combination 
    #   - The segment-pattern crosswalk (may be moot given the above)

# Major Steps:
#   - Filter to rawnav records in applicable stop zones (note that because stop zones will not differ
#       even if several variants of a segment are defined, we don't have to do the segment-by-segment
#       iteration here).
#   - Group by run-segment-stop
#   - Calculate a few new fields:
    #   - Calculate a new column door_state_change that increments every time door_state changes, 
    #     ala R's data.table::rleid() function. 
    #     https://www.rdocumentation.org/packages/data.table/versions/1.12.8/topics/rleid
    #   - Calculate a new column SecsPastSt_marg that is the marginal number of seconds past start 
    #       between the current observation and the next observation.
    #       TODO: this could be calculated earlier - this is a recalculation.
    #   - Calculate a new indicator for door_state_stop_zone that is:
    #          - "open" when door_open == "O" and the earliest door_state_change value among door_open records 
    #          - "closed" otherwise 
    #   - See mark up of a rawnav table at this location for more pointers on how the rest of hte
    #       fields above would show up: https://foursquareitp.sharepoint.com/:x:/r/Shared%20Documents/WMATA%20Queue%20Jump%20Analysis/Client%20Shared%20Folder/data/01-interim/run_basic_decomp_example.xlsx?d=w41a23c96379b4187992714719acbc002&csf=1&web=1&e=YfZHo9
#   - Summarize...


# Calculate Stop-level Baseline Accel-Decel Time (input to t_stop2)
####################################################################################################
#TODO : Update accordingly following more discussion with Burak.

# Would require additional filtering of runs meeting or not meeting certain criteria.

# wr.calc_stop_accel_decel_baseline()

# Calculate t_stop2
####################################################################################################

# TODO: write the wr.calc_tstop2s() function


# 3 Travel Time decomposition
####################################################################################################

# Here, we would call a wr.decompose_traveltime() function. I imagine this would take as input

# TODO: write the wr.decompose_traveltime() function
 


