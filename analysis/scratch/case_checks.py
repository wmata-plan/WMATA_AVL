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
print("Number of rawnav runs in analysis routes is {}".format(len(rawnav_summary_dat)))

# Stop summary
stop_summary = (
    pq.read_table(
        source=os.path.join(path_processed_data, "stop_summary.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
)
print("Number of rawnav runs after stop merge is {}".format(len(stop_summary)))
print(
      "{} runs were removed where distances were less than 2 miles or 10 minutes long."
      .format(len(rawnav_summary_dat) - len(stop_summary))
)

# Segment Summary
# Note: filter by pattern here, so expect a number of runs to drop out
segment_summary = (
    pq.read_table(
        source=os.path.join(path_processed_data, "segment_summary.parquet"),
        use_pandas_metadata = True
    )
    .to_pandas()
)
print("Number of rawnav runs after segment merge is {}".format(len(segment_summary)))
print(
      "{} additional runs were removed because they do not travel in the same direction as the evaluation segment"
      .format(len(stop_summary) - len(segment_summary))
)

# Decomposition 
traveltime_decomp = (
    pd.read_csv(
        os.path.join(path_processed_data,"exports_20200812","traveltime_decomp.csv")
    )   
)
print("Number of rawnav runs before decomposition is {}".format(len(traveltime_decomp)))
print("""{} runs were removed for having pings too far from segment boundaries, 
      for traveling in the wrong direction as the segment, or for having odometer distance
      traveled values inconsistent with the segment length""".format(len(segment_summary) - len(traveltime_decomp)))

# Final
traveltime_decomp_fil = (
    traveltime_decomp
    .loc[(traveltime_decomp.flag_odometer_reset == False) ]    
)
print("Number of ranwav runs decomposed is {}".format(len(traveltime_decomp_fil)))
print(
      """{} runs were removed for failing an apparent odometer reset within the segment"""
      .format(len(traveltime_decomp) - len(traveltime_decomp_fil)))

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

# Load additional files for debugging
stop_index = (
        pq.read_table(source=os.path.join(path_processed_data,"stop_index.parquet"),
                      filters=[[('route','=',route)] for route in analysis_routes],
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
        # As a bit of proofing, we confirm this is int32 and not string, may remove later
        .assign(pattern = lambda x: x.pattern.astype('int32')) 
        .rename(columns = {'odom_ft' : 'odom_ft_qj_stop'})
    ) 
