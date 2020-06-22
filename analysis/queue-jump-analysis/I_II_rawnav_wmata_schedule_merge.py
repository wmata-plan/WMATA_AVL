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

# 1 Import Libraries and Set Global Parameters
########################################################################################################################
# 1.1 Import Python Libraries
############################################
from datetime import datetime

print(f"Run Section 1 Import Libraries and Set Global Parameters...")
begin_time = datetime.now()
import os, sys

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
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Queue Jump Analysis"
    # Source data
    # path_source_data = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\102019 sample")
    path_source_data = r"C:\Downloads"
    path_wmata_schedule_data = os.path.join(path_working, "data", "02-processed")
    # Processed data
    path_processed_data = os.path.join(path_sp, r"Client Shared Folder\data\02-processed")
    path_processed_route_data = os.path.join(path_processed_data, "RouteData")
elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    path_wmata_schedule_data = os.path.join(path_working, "data", "02-processed")
    # Processed data
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
    path_processed_route_data = os.path.join(path_processed_data, "RouteData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, path_wmata_schedule_data, and"
                            " path_processed_data in a new elif block")

# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None
restrict_n = 500
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                     'H8', 'W47']  # 16 Gb RAM can't handle all these at one go
analysis_routes = q_jump_route_list
# analysis_routes = ['70', '64', 'D32', 'H8', 'S2']
# analysis_routes = ['S1', 'S9', 'H4', 'G8', '64']
# analysis_routes = ['S2','S4','H1','H2','H3','79','W47']
# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")
print("*" * 100)

# 2 Analyze Route ---Subset RawNav data.
########################################################################################################################
print(f"Run Section 2 Analyze Route ---Subset RawNav Data...")
begin_time = datetime.now()
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
assert (len(set(day_of_week) - set(analysis_days)) >= 0), print("""
                                                    analysis_days is a subset of following days: 
                                                    ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
                                                """)
# 2.1 Rawnav data
############################################
rawnav_dat = wr.read_cleaned_rawnav(
    analysis_routes_=analysis_routes,
    path_processed_route_data=path_processed_route_data,
    restrict=restrict_n,
    analysis_days=analysis_days)
rawnav_dat = wr.fix_rawnav_names(rawnav_dat)

# 2.2 Summary data
############################################
rawnav_summary_dat, rawnav_trips_less_than_600sec_or_2miles = wr.read_summary_rawnav(
    analysis_routes_=analysis_routes,
    path_processed_route_data=path_processed_route_data,
    restrict=restrict_n,
    analysis_days=analysis_days)
rawnav_summary_dat = wr.fix_rawnav_names(rawnav_summary_dat)
rawnav_summary_keys_col = rawnav_summary_dat[['filename', 'index_trip_start_in_clean_data']]

# 2.3 Merge Processed and Summary data
############################################
rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col, on=['filename', 'index_trip_start_in_clean_data'],
                                    how='right')
rawnav_qjump_dat.pattern = rawnav_qjump_dat.pattern.astype('int')
# Having issues with route "70" and "64"---Getting read as int instead of str
rawnav_qjump_dat.route = rawnav_qjump_dat.route.astype(str)
rawnav_summary_dat.route = rawnav_summary_dat.route.astype(str)
executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Analyze Route ---Subset RawNav Data : {executionTime}")
print("*" * 100)

# 3 Read, analyze and summarize GTFS data
########################################################################################################################
print(f"Run Section 3 Read, analyze and summarize WMATA schedule data...")
begin_time = datetime.now()
# Read the Wmata_Schedule data
wmata_schedule_data_file = os.path.join(path_wmata_schedule_data, 'wmata_schedule_data_q_jump_routes.csv')
wmata_schedule_dat = wr.read_wmata_schedule(wmata_schedule_data_file=wmata_schedule_data_file)
for analysis_route in analysis_routes:
    print("*" * 100)
    print(f'Processing analysis route {analysis_route}')
    for analysis_day in analysis_days:
        print(f'Processing {analysis_day}')
        data_exist_dir = \
            os.path.join(path_processed_data, 'wmata_schedule_based_sum_dat', str(analysis_route), analysis_day)
        if os.path.isdir(data_exist_dir):
            print(f'Skipping analysis route {analysis_route} for {analysis_day}: already processed')
            continue
        print("*" * 50)
        print(f'Processing analysis route {analysis_route} for {analysis_day}...')
        wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
            wr.parent_merge_rawnav_wmata_schedule(
                analysis_route_=analysis_route,
                analysis_day_=analysis_day,
                rawnav_dat_=rawnav_qjump_dat,
                rawnav_sum_dat_=rawnav_summary_dat,
                wmata_schedule_dat_=wmata_schedule_dat)
        if type(wmata_schedule_based_sum_dat) == type(None):
            print(f'No data on analysis route {analysis_route} for {analysis_day}')
            continue

        wr.output_rawnav_wmata_schedule(
            analysis_route_=analysis_route,
            analysis_day_=analysis_day,
            wmata_schedule_based_sum_dat_=wmata_schedule_based_sum_dat,
            rawnav_wmata_schedule_dat=nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat,
            path_processed_data_=path_processed_data)
executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 3 Read, analyze and summarize WMATA schedule data : {executionTime}")
print("*" * 100)
