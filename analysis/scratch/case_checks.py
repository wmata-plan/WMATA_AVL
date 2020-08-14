# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 08:05:16 2020

@author: WylieTimmerman
"""
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
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# EPSG code for WMATA-area work
wmata_crs = 2248

import wmatarawnav as wr

# Summary
# 37720 runs on our analysis routes
rawnav_summary_dat = (
    wr.read_cleaned_rawnav(
        analysis_routes_ = analysis_routes,
        analysis_days_ = analysis_days,
        path = os.path.join(path_processed_data, "rawnav_summary.parquet")
    )
)

# Stop summary
stop_summary = (
    pq.read_table(
        source=os.path.join(path_processed_data, "stop_summary.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
)
# 35041 runs remain

# Segment Summary
# Note: filter by pattern here, so expect a number of runs to drop out
segment_summary = (
    pq.read_table(
        source=os.path.join(path_processed_data, "segment_summary.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
)
# valid records remaining: 22871 (number that appear in traveltime_decomp) 

# Decomposition 
traveltime_decomp = (
    pd.read_csv(
        os.path.join(path_processed_data,"exports_20200812","traveltime_decomp.csv")
    )   
)

# Final
traveltime_decomp_fil = (
    traveltime_decomp
    .loc[(traveltime_decomp.flag_failed_qj_stop_merge == False) & (traveltime_decomp.flag_odometer_reset == False) ]    
)
# End with 18037 valid runs (number that appear in traveltime_decomp_fil)


# why did segment filtering happen?
segment_cases = (
    segment_summary
    .agg({'flag_too_far_any':['sum'],
          'flag_wrong_order_any':['sum'],
          'flag_too_long_odom' :['sum']}
    )
    .pipe(wr.reset_col_names)
          
 )

traveltime_cases = (
    traveltime_decomp
    .agg({'flag_failed_qj_stop_merge':['sum'],
          'flag_odometer_reset':['sum']}
    )
    .pipe(wr.reset_col_names)
)

