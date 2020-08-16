# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 02:22:39 2020

@author: WylieTimmerman, ApoorbaBibeka
"""

# 0 Housekeeping. Clear variable space
########################################################################################################################
from IPython import get_ipython  # run magic commands

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
import os, sys, shutil
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
    path_segments = os.path.join(path_working,"data","02-processed")
elif os.getlogin() == "abibeka":
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
    path_segments = path_processed_data
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

# Globals
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', 
                     '70', '79', 
                     '64', 'G8', 
                     'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
analysis_routes = q_jump_route_list
# analysis_routes = ['S1']
# analysis_routes = ['S1', 'S9', 'H4', 'G8', '64']
# analysis_routes = ['S2','S4','H1','H2','H3','79','W47']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# analysis_days = ['Wednesday','Thursday','Friday']


# EPSG code for WMATA-area work
wmata_crs = 2248
# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

# 2 Load Relevant Static Files 
####################################################################################################

# 2.1. Load segment-pattern-stop crosswalk 
# This crosswalk is used to connect segment shapes to rawnav data. The 'route' field must 
# match the same string used in the rawnav data. 'direction' will be looked up in a moment
# against the wmata schedule database
# and replaced with a pattern code as an int32 value. 'seg_name_id' is also found in the segment
# geometry file. 'stop_id' matches the stop identifier in the WMATA schedule database.
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
        path = os.path.join(path_source_data,
                            "wmata_schedule_data",
                            "Schedule_082719-201718.mdb"),
        analysis_routes = analysis_routes)
    .filter(items = ['direction', 'route','pattern'])
    .drop_duplicates()
)
    
xwalk_seg_pattern_stop = (
    xwalk_seg_pattern_stop_in
    .merge(
        xwalk_wmata_route_dir_pattern, 
        on = ['route','direction']
    )
    .drop('direction', 1)
    .reindex(columns = ['route','pattern','seg_name_id','stop_id'])
)

del xwalk_seg_pattern_stop_in

xwalk_seg_pattern = (
    xwalk_seg_pattern_stop
    .drop('stop_id', 1)
    .drop_duplicates()
)

# 3. load shapes
# Segments
# Note unique identifier seg_name_id. Other fields are optional
# Note that these are not yet updated to reflect the extension of the 11th street segment 
# further south to give the stop more breathing room.
segments = (
    gpd.read_file(os.path.join(path_segments,"segments.geojson"))
    .to_crs(wmata_crs)
)

# 3 Merge Additional Geometry
####################################################################################################

# 3.1 Rawnav-Segment ########################

# Make Output Directory
path_seg_summary = os.path.join(path_processed_data, "segment_summary.parquet")
shutil.rmtree(path_seg_summary, ignore_errors=True) 
os.mkdir(path_seg_summary)

path_seg_index = os.path.join(path_processed_data, "segment_index.parquet")
shutil.rmtree(path_seg_index, ignore_errors=True) 
os.mkdir(path_seg_index)

# Iterate
for analysis_route in analysis_routes:
    print("*" * 100)
    print(f'Processing analysis route {analysis_route}')
    for analysis_day in analysis_days:
        print(f'Processing analysis route {analysis_route} for {analysis_day}...')
        
        # Reload data
        try:
            rawnav_dat = (
                wr.read_cleaned_rawnav(
                   analysis_routes_ = analysis_route,
                   analysis_days_ = analysis_day,
                   path = os.path.join(path_processed_data, "rawnav_data.parquet"))
                .drop(columns=['blank', 'lat_raw', 'long_raw', 'sat_cnt'])
                )
        except:
            print(f'No data on analysis route {analysis_route} for {analysis_day}')
            continue
        else:
   
            # Reload Data
            rawnav_summary_dat = (
                wr.read_cleaned_rawnav(
                    analysis_routes_ = analysis_route,
                    analysis_days_ = analysis_day,
                    path = os.path.join(path_processed_data, "rawnav_summary.parquet")
                )
            )

            # Subset Rawnav Data to Records Desired
            rawnav_summary_dat = rawnav_summary_dat.query('not (run_duration_from_sec < 600 | dist_odom_mi < 2)')
            
            rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_dat[['filename', 'index_run_start']], 
                                                on=['filename', 'index_run_start'],
                                                how='right')
            
            # Address Remaining Col Format issues
            rawnav_qjump_gdf = (
                gpd.GeoDataFrame(
                    rawnav_qjump_dat, 
                    geometry = gpd.points_from_xy(
                        rawnav_qjump_dat.long,
                        rawnav_qjump_dat.lat
                    ),
                    crs='EPSG:4326')
                .to_crs(epsg=wmata_crs)
            )
    
            # Iterate on over Pattern-Segments Combinations Applicable to Route
            xwalk_seg_pattern_subset = xwalk_seg_pattern.query('route == @analysis_route')
                        
            for seg in xwalk_seg_pattern_subset.seg_name_id.unique():
                print('Processing segment {} ...'.format(seg))

                # We pass the rawnav data and summary tables, check against a segment,
                # and use the patterns_by_seg to indicate which patterns should be examined
                index_run_segment_start_end, summary_run_segment = (
                    wr.merge_rawnav_segment(
                        rawnav_gdf_=rawnav_qjump_gdf,
                        rawnav_sum_dat_=rawnav_summary_dat,
                        target_=segments.loc[segments.seg_name_id == seg],
                        patterns_by_seg_=xwalk_seg_pattern_subset.loc[xwalk_seg_pattern_subset.seg_name_id == seg]
                    )
                )
                # Note that because seg_pattern_first_last is defined for route and pattern,
                # our summary will implicitly drop any runs that are on 'wrong' pattern(s) for 
                # a route. 
                
                index_run_segment_start_end['wday'] = analysis_day
                summary_run_segment['wday'] = analysis_day
                
                # The additional partitioning here is excessive, but if fits better in the 
                # iterative/chunking process above
                pq.write_to_dataset(
                    table = pa.Table.from_pandas(summary_run_segment),
                    root_path = path_seg_summary,
                    partition_cols = ['route','wday','seg_name_id']
                )
                
                pq.write_to_dataset(
                    table = pa.Table.from_pandas(index_run_segment_start_end),
                    root_path = path_seg_index,
                    partition_cols = ['route','wday','seg_name_id']
                )
                

