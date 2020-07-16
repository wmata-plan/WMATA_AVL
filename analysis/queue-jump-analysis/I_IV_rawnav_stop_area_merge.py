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
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', 
                     '70', '79', 
                     '64', 'G8', 
                     'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
analysis_routes = q_jump_route_list
# analysis_routes = ['70', '64', 'D32', 'H8', 'S2']
# analysis_routes = ['S1', 'S9', 'H4', 'G8', '64']
# analysis_routes = ['S2','S4','H1','H2','H3','79','W47']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# EPSG code for WMATA-area work
wmata_crs = 2248

# How many feet ahead and behind to define the stop zone
look_back_ft = 150
look_forward_ft = 150

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
             ['route',        'direction',                    'seg_name_id','stop_id'], 
                 "79",            "SOUTH",               "georgia_columbia",   10981, 
                 "79",            "SOUTH",      "georgia_piney_branch_long",    4217, 
                 #not sure yet how to deal with second stop, but i think this works
                 "70",            "SOUTH",                 "georgia_irving",   19186, #irving stop
                 "70",            "SOUTH",                 "georgia_irving",   10981, #columbia stop 
                 "70",            "SOUTH",      "georgia_piney_branch_shrt",    4217,
                 "S1",            "NORTH",               "sixteenth_u_shrt",   18042,
                 "S2",            "NORTH",               "sixteenth_u_shrt",   18042,
                 "S4",            "NORTH",               "sixteenth_u_shrt",   18042,
                 "S9",            "NORTH",               "sixteenth_u_long",   18042,
                 "64",            "NORTH",            "eleventh_i_new_york",   16490,
                 "G8",             "EAST",            "eleventh_i_new_york",   16490,
                "D32",             "EAST",     "irving_fifteenth_sixteenth",    2368,
                 "H1",            "NORTH",     "irving_fifteenth_sixteenth",    2368,
                 "H2",             "EAST",     "irving_fifteenth_sixteenth",    2368,
                 "H3",             "EAST",     "irving_fifteenth_sixteenth",    2368,
                 "H4",             "EAST",     "irving_fifteenth_sixteenth",    2368,
                 "H8",             "EAST",     "irving_fifteenth_sixteenth",    2368,
                "W47",             "EAST",     "irving_fifteenth_sixteenth",    2368
  )


xwalk_wmata_route_dir_pattern = wr.read_sched_db_patterns(
    path = os.path.join(path_source_data,
                        "wmata_schedule_data",
                        "Schedule_082719-201718.mdb"),
    analysis_routes = analysis_routes)\
    [['direction', 'route','pattern']]\
    .drop_duplicates()
    
xwalk_seg_pattern_stop = (xwalk_seg_pattern_stop_in
                          .merge(xwalk_wmata_route_dir_pattern, on = ['route','direction'])
                          .drop('direction', 1)
                          .reindex(columns = ['route','pattern','seg_name_id','stop_id']))

del xwalk_seg_pattern_stop_in

# 2. load segment-pattern crosswalk (could be loaded separately or summarized from above)
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


# 2.2 Segment Reformat ########################
# A bit of a hack for now, but we'll reformat the segments file and segments crosswalk to make a 
# file similar in key to the wmata patterns file. Then we'll be able to reuse that function in 
# a number of ways.

seg_pattern_shape = segments.merge(xwalk_seg_pattern, on = ['seg_name_id'])

seg_pattern_first_last = wr.explode_first_last(seg_pattern_shape)

# GO GO ANALYZE
####################################

for seg in list(xwalk_seg_pattern.seg_name_id.drop_duplicates()):
    print('now on {}'.format(seg))
    
    # Begin subset
    xwalk_seg_pattern_stop_fil = xwalk_seg_pattern_stop.query('seg_name_id == @seg')
    
    seg_routes = list(xwalk_seg_pattern_stop_fil.route.drop_duplicates())

    # Load stop index
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
                      use_pandas_metadata = True)
        .to_pandas()
        .assign(pattern = lambda x: x.pattern.astype('int32')) #  pattern is object not int? # TODO: fix
        .rename(columns = {'odom_ft' : 'odom_ft_qj_stop'})
    ) 
    
    # Filter Stop index to the relevant QJ stops
    stop_index_fil = (
        stop_index
        .merge(xwalk_seg_pattern_stop_fil,
               on = ['route','pattern','stop_id'],
               how = 'inner')   
    )

    # Read the rawnav data
    rawnav_dat = (
        wr.read_cleaned_rawnav(
           analysis_routes_ = seg_routes,     
           path = os.path.join(path_processed_data, "rawnav_data.parquet"))
        .drop(columns=['blank', 'lat_raw', 'long_raw', 'sat_cnt'])
    )

    # Add the QJ stop identifiers in
    rawnav_add_stop_detail = (
        rawnav_dat
        .merge(stop_index_fil
               .filter(items = ['filename','index_run_start','odom_ft_qj_stop']),
               on = ['filename','index_run_start'],
               how = "inner")
    )
    
    # Filter to the window
    rawnav_add_stop_detail_filter = (
        rawnav_add_stop_detail
        .query('odom_ft >= (odom_ft_qj_stop - 150) & odom_ft < (odom_ft_qj_stop + 150)')
    )
    
        
    
    # On a rolling basis, look for the points X feet behind and Y feet ahead 
    
    breakpoint()
    
    print('himom')

