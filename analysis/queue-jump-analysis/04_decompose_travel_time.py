# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 03:39:56 2020

@author: WylieTimmerman, Apoorba Bibeka

Note: the sections here are guideposts - this may be better as several scripts
"""

# 0 Housekeeping. Clear variable space
######################################
from IPython import get_ipython  

ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")

# 1 Import Libraries and Set Global Parameters
##############################################
# 1.1 Import Python Libraries
#############################
from datetime import datetime
print("Run Section 1 Import Libraries and Set Global Parameters...")
begin_time = datetime.now()

import os, sys
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq

# 1.2 Set Global Parameters
###########################

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
     
# 1.3 Import User-Defined Package
#################################
import wmatarawnav as wr

# 1.4 Reload Relevant Files 
###########################
# Load segment-pattern-stop crosswalk 
# This crosswalk is used to connect segment shapes to rawnav data. 
# - The 'route' field must match the same string used in the rawnav data. 
# - 'direction' will be looked up in a moment against the WMATA schedule database and 
#       replaced with a pattern code as an int32 value. 
# - 'seg_name_id' is also found in the segment geometry file. 
# - 'stop_id' matches the stop identifier in the WMATA schedule database.
xwalk_seg_pattern_stop_in = wr.tribble(
             ['route',        'direction',                        'seg_name_id','stop_id'], 
                 "79",            "SOUTH",               "georgia_columbia_stub",   10981, 
                 "79",            "SOUTH",           "georgia_piney_branch_stub",    4217, 
                 "70",            "SOUTH",                 "georgia_irving_stub",   19186, #irving stop
                 "70",            "SOUTH",               "georgia_columbia_stub",   10981, #columbia stop 
                 "70",            "SOUTH",           "georgia_piney_branch_stub",    4217,
                 "S1",            "NORTH",                    "sixteenth_u_stub",   18042,
                 "S2",            "NORTH",                    "sixteenth_u_stub",   18042,
                 "S4",            "NORTH",                    "sixteenth_u_stub",   18042,
                 "S9",            "NORTH",                    "sixteenth_u_stub",   18042,
                 "64",            "NORTH",            "eleventh_i_new_york_stub",   16490,
                 "G8",             "EAST",            "eleventh_i_new_york_stub",   16490,
                "D32",             "EAST",     "irving_fifteenth_sixteenth_stub",    2368,
                 "H1",            "NORTH",     "irving_fifteenth_sixteenth_stub",    2368,
                 "H2",             "EAST",     "irving_fifteenth_sixteenth_stub",    2368,
                 "H3",             "EAST",     "irving_fifteenth_sixteenth_stub",    2368,
                 "H4",             "EAST",     "irving_fifteenth_sixteenth_stub",    2368,
                 "H8",             "EAST",     "irving_fifteenth_sixteenth_stub",    2368,
                "W47",             "EAST",     "irving_fifteenth_sixteenth_stub",    2368
  )

xwalk_wmata_route_dir_pattern = (
    wr.read_sched_db_patterns(
        path = os.path.join(
            path_source_data,
           "wmata_schedule_data",
           "Schedule_082719-201718.mdb"
           ),
        analysis_routes = analysis_routes
    )
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

# 2. Decompose Travel Time
##########################

# 2.0 Setup exports
###################
freeflow_list = []
stop_area_decomp_list = []
traveltime_decomp_list = []

path_exports = (
    os.path.join(
        path_processed_data,
        "exports_{}".format(datetime.now().strftime("%Y%m%d"))
    )
)
if not os.path.isdir(path_exports):
    os.mkdir(path_exports)

for seg in list(xwalk_seg_pattern_stop.seg_name_id.drop_duplicates()): #["eleventh_i_new_york"]: #list(xwalk_seg_pattern_stop.seg_name_id.drop_duplicates()):
# Iterate over Segments
    print('now on {}'.format(seg))
    # 2.1. Read-in Data 
    ###################
    # Reduce rawnav data to runs present in the summary file after filtering.
    
    xwalk_seg_pattern_stop_fil = xwalk_seg_pattern_stop.query('seg_name_id == @seg')

    seg_routes = list(xwalk_seg_pattern_stop_fil.route.drop_duplicates())
    
    rawnav_dat = (
        wr.read_cleaned_rawnav(
           analysis_routes_ = seg_routes,
           path = os.path.join(path_processed_data, "rawnav_data.parquet")
        )
        .drop(columns=['blank', 'lat_raw', 'long_raw', 'sat_cnt'])
    )
            
    segment_summary = (
        pq.read_table(
            source = os.path.join(path_processed_data,"segment_summary_2017_test.parquet"),
            filters = [['seg_name_id', "=", seg]],
            use_pandas_metadata = True
        )
        .to_pandas()
    )

    segment_summary_fil = (
        segment_summary
        .query('~(flag_too_far_any\
                  | flag_wrong_order_any\
                  | flag_too_long_odom\
                  | flag_secs_total_mismatch\
                  | flag_odom_total_mismatch)'
        )
    )
       
    stop_index = (
        pq.read_table(source=os.path.join(path_processed_data,"stop_index.parquet"),
                      filters=[[('route','=',route)] for route in seg_routes],
                        columns = ['seg_name_id',
                                    'route',
                                    'pattern',
                                    'stop_id',
                                    'filename',
                                    'index_run_start',
                                    'index_loc',
                                    'odom_ft',
                                    'sec_past_st',
                                    'geo_description'],
                      use_pandas_metadata = True
        )
        .to_pandas()
        # As a bit of proofing, we confirm this is int32 and not string, may remove later
        .assign(pattern = lambda x: x.pattern.astype('int32')) 
        .rename(columns = {'odom_ft' : 'odom_ft_qj_stop'})
    ) 
    
    # Filter Stop index to the relevant QJ stops
    stop_index_fil = (
        stop_index
        .merge(xwalk_seg_pattern_stop_fil,
               on = ['route','pattern','stop_id'],
               how = 'inner')   
    )
     
    # 2.2 Run Decomposition Functions
    #################################
    # These functions could be further nested within one another, but are kept separate to support
    # easier review of intermediate outputs and to allow them to be used independently if needed.
    # As a result, data is in some cases filtered several times, but the overall time loss is 
    # small enough to be immaterial.
    
    # Calculate Free Flow Travel Time through Entire Segment
    segment_ff = (
        wr.decompose_segment_ff(
            rawnav_dat,
            segment_summary_fil,
            max_fps = 73.3
        )
        .assign(seg_name_id = seg)
    )
                
    freeflow_list.append(segment_ff)
    
    # Calculate Stop-Area Decomposition
    stop_area_decomp = (
        wr.decompose_stop_area(
            rawnav_dat,
            segment_summary_fil,
            stop_index_fil
        )
        .assign(seg_name_id = seg)
    )
    
    stop_area_decomp_list.append(stop_area_decomp)
    
    segment_ff_val = (
        segment_ff
        .loc[0.95]
        .loc["fps_next3"]
    )

    # Run decomposition
    traveltime_decomp = (
        wr.decompose_traveltime(
            rawnav_dat,
            segment_summary_fil,
            stop_area_decomp,
            segment_ff_val
        )
    )
    traveltime_decomp_list.append(traveltime_decomp)
  

# 3. Export
###########

# 3.1 Combine results by segment into tables   
########################################
freeflow = (
    pd.concat(freeflow_list)
    .rename_axis('ntile')
    .reset_index()
)

basic_decomp = (
    pd.concat(stop_area_decomp_list)
    .reset_index() 
)

traveltime_decomp = (
    pd.concat(traveltime_decomp_list)
    .reset_index()
)

# 3.2 Export Values
###################

freeflow.to_csv(os.path.join(path_exports,"freeflow.csv"))

basic_decomp.to_csv(os.path.join(path_exports,"basic_decomp.csv"))

traveltime_decomp.to_csv(os.path.join(path_exports,"traveltime_decomp.csv"))
