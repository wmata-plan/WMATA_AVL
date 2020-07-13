# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Purpose: Merge wmata_schedule and rawnav data
Created on Fri May 15 15:36:49 2020
"""

# 0 Housekeeping. Clear variable space
########################################################################################################################
from IPython import get_ipython  # run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()
#https://stackoverflow.com/questions/36572282/ipython-autoreload-magic-function-not-found
ipython.magic("load_ext autoreload")

# 1 Import Libraries and Set Global Parameters
########################################################################################################################
# 1.1 Import Python Libraries
############################################
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
print(f"Run Section 1 Import Libraries and Set Global Parameters...")
begin_time = datetime.now()
import os, sys, pandas as pd, geopandas as gpd

if not sys.warnoptions:
    import warnings

    warnings.simplefilter("ignore")  # Stop Pandas warnings

# 1.2 Set Global Parameters
############################################
if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\Users\WylieTimmerman\Documents\projects_local\wmata_avl_local"
    path_source_data = os.path.join(path_sp,"data","00-raw")
    path_processed_data = os.path.join(path_sp, "data","02-processed")
elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    # Processed data
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, and"
                            " path_processed_data in a new elif block")

# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None
restrict_n = None
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                     'H8', 'W47']
analysis_routes = q_jump_route_list
# analysis_routes = ['70', '64', 'D32', 'H8', 'S2']
# analysis_routes = ['S1', 'S9', 'H4', 'G8', '64']
# analysis_routes = ['S2','S4','H1','H2','H3','79','W47']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")
print("*" * 100)

# 2 Read, analyze and summarize Schedule data
########################################################################################################################
print(f"Run Section 2: Read, analyze and summarize rawnav, WMATA schedule data...")
begin_time = datetime.now()
# Read the Wmata_Schedule data
wmata_schedule_dat = wr.read_sched_db_patterns(
    path = os.path.join(path_source_data,
                        "wmata_schedule_data",
                        "Schedule_082719-201718.mdb"),
    analysis_routes = analysis_routes)

wmata_schedule_gdf = gpd.GeoDataFrame(
            wmata_schedule_dat, 
            geometry = gpd.points_from_xy(wmata_schedule_dat.stop_lon,wmata_schedule_dat.stop_lat),
            crs='EPSG:4326').\
            to_crs(epsg=wmata_crs)

# Make Output Directory
# TODO: Function or something that makes this safer?
path_stop_summary = os.path.join(path_processed_data, "stop_summary.parquet")
shutil.rmtree(path_stop_summary, ignore_errors=True)
os.mkdir(path_stop_summary)

path_stop_index = os.path.join(path_processed_data, "stop_index.parquet")
shutil.rmtree(path_stop_index, ignore_errors=True)
os.mkdir(path_stop_index)

for analysis_route in analysis_routes:
    print("*" * 100)
    print(f'Processing analysis route {analysis_route}')
    for analysis_day in analysis_days:
        print(f'Processing {analysis_day}')
        print("*" * 50)
        print(f'Processing analysis route {analysis_route} for {analysis_day}...')
        # Reload data
        try:
            rawnav_dat = wr.read_cleaned_rawnav(
                path_processed_route_data=os.path.join(path_processed_data, "RouteData"),
                analysis_routes_=analysis_route,
                analysis_days_=analysis_day,
                restrict=restrict_n)
        except Exception as e:
            print(e)  # usually no data found or something similar
            continue
        else:
            rawnav_dat = wr.fix_rawnav_names(rawnav_dat)
            rawnav_summary_dat, rawnav_trips_less_than_600sec_or_2miles = wr.read_summary_rawnav(
                path_processed_route_data=os.path.join(path_processed_data, "RouteData"),
                analysis_routes_=analysis_route,
                analysis_days_=analysis_day,
                restrict=restrict_n)
            rawnav_summary_dat = wr.fix_rawnav_names(rawnav_summary_dat)
            # Subset Rawnav Data to Records Desired
            rawnav_summary_keys_col = rawnav_summary_dat[['filename', 'index_trip_start_in_clean_data']]
            rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col,
                                                on=['filename', 'index_trip_start_in_clean_data'],
                                                how='right')

            # Address Remaining Col Format issues
            # TODO: resolve these elsewhere
            rawnav_qjump_dat.pattern = rawnav_qjump_dat.pattern.astype('int')
            rawnav_qjump_dat.route = rawnav_qjump_dat.route.astype(str)
            rawnav_summary_dat.route = rawnav_summary_dat.route.astype(str)

            rawnav_qjump_gdf = gpd.GeoDataFrame(
                rawnav_qjump_dat,
                geometry=gpd.points_from_xy(rawnav_qjump_dat.long, rawnav_qjump_dat.lat),
                crs='EPSG:4326'). \
                to_crs(epsg=wmata_crs)

        wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
            wr.merge_rawnav_wmata_schedule(
                analysis_route_=analysis_route,
                analysis_day_=analysis_day,
                rawnav_dat_=rawnav_qjump_gdf,
                rawnav_sum_dat_=rawnav_summary_dat,
                wmata_schedule_dat_=wmata_schedule_gdf)
        if type(wmata_schedule_based_sum_dat) == type(None):
            print(f'No data on analysis route {analysis_route} for {analysis_day}')
            continue

        pq.write_to_dataset(
            table=pa.Table.from_pandas(wmata_schedule_based_sum_dat),
            root_path=path_stop_summary,
            partition_cols=['route', 'wday'])
        nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat=\
            wr.drop_geometry(nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat)
        nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat=\
            nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat.assign(wday=analysis_day)
        pq.write_to_dataset(
            table=pa.Table.from_pandas(nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat),
            root_path=path_stop_index,
            partition_cols=['route', 'wday'])

executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section Section 2: Read, analyze and summarize rawnav, WMATA schedule data : {executionTime}")
print("*" * 100)
