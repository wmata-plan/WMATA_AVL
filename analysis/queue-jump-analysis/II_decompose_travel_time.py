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
# Queue Jump Routes
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', 
                     '70', '79', 
                     '64', 'G8', 
                     'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
analysis_routes = q_jump_route_list
# EPSG code for WMATA-area work
wmata_crs = 2248
     
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
                 # "64",            "NORTH",                    "nh_3rd_test",   17329, #4th street
                 # "64",            "NORTH",                    "nh_3rd_test",   25370, #3rd street
                 # "G8",             "EAST",                 "ri_lincoln_test",  26282
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

# 3. load shapes
# Segments
# Note unique identifier seg_name_id
# Note that these are not yet updated to reflect the extension of the 11th street segment 
# further south to give the stop more breathing room.
segments = (
    gpd.read_file(os.path.join(path_segments,"segments.geojson"))
    .to_crs(wmata_crs)
)
    
# 3 Filter Out Runs that Appear Problematic
###########################################\
nonstopzone_freeflow_list = []
freeflow_list = []
basic_decomp_list = []

# Set up folder to dump results to
# TODO: improve path / save behavior
path_stop_area_dump = os.path.join(path_processed_data,"rawnav_stop_areas")
if not os.path.isdir(path_stop_area_dump ):
    os.mkdir(path_stop_area_dump )

for seg in list(xwalk_seg_pattern_stop.seg_name_id.drop_duplicates()): #["eleventh_i_new_york"]: #list(xwalk_seg_pattern_stop.seg_name_id.drop_duplicates()):
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
    
    # TODO: fix the segment code so this is unneccessary -- not sure why this is doing this now
    # Update: Should be fixed, can remove later after testing to confirm
    segment_summary = segment_summary[~segment_summary.duplicated(['filename', 'index_run_start'], keep='last')] 
    
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
        .assign(pattern = lambda x: x.pattern.astype('int32')) #  pattern is string not int? # TODO: fix
        .rename(columns = {'odom_ft' : 'odom_ft_qj_stop'})
    ) 
    
    # NOTE: this would be the place to filter out runs where a high number of stops don't match
    # but we're okay with runs that we don't have complete data on
    # Filter Stop index to the relevant QJ stops
    stop_index_fil = (
        stop_index
        .merge(xwalk_seg_pattern_stop_fil,
               on = ['route','pattern','stop_id'],
               how = 'inner')   
    )
    
    # Run Decomposition Support Functions
    #############################
    # Calculate Free Flow outside Stop Area
    # FYI: Currently, this is largely for the 'alternative' decomposition, may do 
    # things with this later. Results are for now loaded into R where we incorporate
    # that into the remaining decomp stuff.
    nonstopzone_ff = (
        wr.decompose_nonstoparea_ff(rawnav_dat,
                                    segment_summary,
                                    stop_index_fil,
                                    max_fps = 73.3)
        .assign(seg_name_id = seg)
    )
    
    nonstopzone_freeflow_list.append(nonstopzone_ff)

    
    # Calculate Free Flow Travel Time
    segment_ff = (
        wr.decompose_segment_ff(rawnav_dat,
                                segment_summary,
                                max_fps = 73.3)
        .assign(seg_name_id = seg)
    )
                
    freeflow_list.append(segment_ff)
    
    # Calculate Stop-Area Decomposition
    rawnav_fil_stop_area_decomp = (
        wr.decompose_stop_area(rawnav_dat,
                               segment_summary,
                               stop_index_fil)
        .assign(seg_name_id = seg)
    )
    
    basic_decomp_list.append(rawnav_fil_stop_area_decomp)
    
    #ENDS HERE
    
nonstopzone_freeflow = (
    pd.concat(nonstopzone_freeflow_list)
    .reset_index()
)
    
freeflow = (
    pd.concat(freeflow_list)
    .rename_axis('ntile')
    .reset_index()
)

basic_decomp = (
    pd.concat(basic_decomp_list)
    .reset_index() 
)

# Quick dump of values
#####################
# TODO: improve path / save behavior
freeflow.to_csv(os.path.join(path_stop_area_dump,"freeflow.csv"))

nonstopzone_freeflow.to_csv(os.path.join(path_stop_area_dump,"nonstopzone_ff.csv"))

basic_decomp.to_csv(os.path.join(path_stop_area_dump,"basic_decomp.csv"))

# Calculate t_stop2
####################################################################################################

# TODO: write the wr.calc_tstop2s() function


# 3 Travel Time decomposition
####################################################################################################

# Here, we would call a wr.decompose_traveltime() function. I imagine this would take as input

# TODO: write the wr.decompose_traveltime() function
 


