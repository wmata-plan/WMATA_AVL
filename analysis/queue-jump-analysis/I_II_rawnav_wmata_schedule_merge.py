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
import pandas as pd, os, sys, shutil
import pyarrow.parquet as pq
import pyarrow as pa

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
    # Source Data
    # path_source_data = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\102019 sample")
    path_source_data = r"C:\Downloads"
    path_wmata_schedule_data = os.path.join(path_working, "data", "02-processed")
    # Processed Data
    path_processed_data = os.path.join(path_sp, r"Client Shared Folder\data\02-processed")
    path_processed_route_data = os.path.join(path_processed_data, "RouteData")
elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source Data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    path_wmata_schedule_data = os.path.join(path_working, "data", "02-processed")
    # Processed Data
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
    path_processed_route_data = os.path.join(path_processed_data, "RouteData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, path_wmata_schedule_data, and"
                            " path_processed_data in a new elif block")

# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None
restrict_n = None
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                     'H8','W47']  # 16 Gb RAM can't handle all these at one go
analysis_routes = q_jump_route_list
# analysis_routes = ['70', '64', 'D32', 'H8', 'S2']
#analysis_routes = ['S1', 'S9', 'H4', 'G8', '64']
# analysis_routes = ['G8']
# analysis_routess = ['S2','S4','H1','H2','H3','79','W47']

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr
executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")
print("*"*100)

# 2 Analyze Route ---Subset RawNav Data.
########################################################################################################################
print(f"Run Section 2 Analyze Route ---Subset RawNav Data...")
begin_time = datetime.now()
analysis_days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
day_of_week = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
assert(len(set(day_of_week)-set(analysis_days))>= 0), print("""
                                                    analysis_days is a subset of following days: 
                                                    ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
                                                """)
# 2.1 Rawnav Data
############################################
rawnav_dat = wr.read_processed_rawnav(
    analysis_routes_=analysis_routes,
    path_processed_route_data=path_processed_route_data,
    restrict=restrict_n,
    analysis_days=analysis_days)
rawnav_dat = wr.fix_rawnav_names(rawnav_dat)

# 2.2 Summary Data
############################################
rawnav_summary_dat, rawnav_trips_less_than_600sec_or_2miles = wr.read_summary_rawnav(
    analysis_routes_=analysis_routes,
    path_processed_route_data=path_processed_route_data,
    restrict=restrict_n,
    analysis_days=analysis_days)
rawnav_summary_dat = wr.fix_rawnav_names(rawnav_summary_dat)
rawnav_summary_keys_col = rawnav_summary_dat[['filename','index_trip_start_in_clean_data']]

# 2.3 Merge Processed and Summary Data
############################################
rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col,on=['filename','index_trip_start_in_clean_data'],how='right')
rawnav_qjump_dat.pattern = rawnav_qjump_dat.pattern.astype('int')
set(rawnav_qjump_dat.index_trip_start_in_clean_data.unique()) -set(rawnav_summary_dat.index_trip_start_in_clean_data.unique())
executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Analyze Route ---Subset RawNav Data : {executionTime}")
print("*"*100)

# 3 Read, analyze and summarize GTFS Data
########################################################################################################################
print(f"Run Section 3 Read, analyze and summarize WMATA schedule Data...")
begin_time = datetime.now() ##
# 3.1 Read the Wmata_Schedule Data
############################################
wmata_schedule_data_file = os.path.join(path_wmata_schedule_data,'wmata_schedule_data_q_jump_routes.csv')
wmata_schedule_dat = pd.read_csv(wmata_schedule_data_file,index_col=0).reset_index(drop=True)
wmata_schedule_dat.rename(columns = {'cd_route':'route','cd_variation':'pattern',
                                      'longitude':'stop_lon','latitude':'stop_lat',
                                     'stop_dist':'dist_from_previous_stop'},inplace=True)

# 3.2 Merge all stops to rawnav data
############################################
# import importlib
# importlib.reload(wr)
# import inspect
# print(inspect.getsource(wr.include_wmata_schedule_based_summary))


nearest_rawnav_point_to_wmata_schedule_dat = \
    wr.merge_stops_wmata_schedule_rawnav(
        wmata_schedule_dat = wmata_schedule_dat ,
        rawnav_dat =rawnav_qjump_dat
)
nearest_rawnav_point_to_wmata_schedule_dat.rename(columns = {'heading':'stop_heading'},inplace=True)
nearest_rawnav_point_to_wmata_schedule_dat = \
    wr.remove_stops_with_dist_over_100ft(nearest_rawnav_point_to_wmata_schedule_dat)
# Assert and clean stop data
nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat =\
    wr.assert_clean_stop_order_increase_with_odom(nearest_rawnav_point_to_wmata_schedule_dat)

wmata_schedule_based_sum_dat= wr.include_wmata_schedule_based_summary(
    rawnav_q_dat = rawnav_qjump_dat,
    rawnav_sum_dat = rawnav_summary_dat,
    nearest_stop_dat =nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat
)
wmata_schedule_based_sum_dat.\
    set_index(['fullpath','filename','file_id','wday','start_date_time','end_date_time',
               'index_trip_start_in_clean_data','taglist','route_pattern','route','pattern'],inplace=True,drop=True)

# 3.6 Output Summary Files
############################################
sum_out_file = os.path.join(path_processed_data,f'wmata_schedule_trip_summaries.xlsx')
wmata_schedule_based_sum_dat.to_excel(sum_out_file,merge_cells=False)

# 3.7 Output GTFS+Rawnav Merged Files
############################################
wmata_schedule_rawnav_out_file = os.path.join(path_processed_data,f'wmata_schedule_stop_locations_inventory.parquet')
nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat.drop(columns='geometry',inplace=True)
table_from_pandas = pa.Table.from_pandas(nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat)
while os.path.isdir(wmata_schedule_rawnav_out_file):
    shutil.rmtree(wmata_schedule_rawnav_out_file, ignore_errors=True)  # Remove data from RemFolder before writing
pq.write_to_dataset(table_from_pandas, root_path= wmata_schedule_rawnav_out_file, \
                    partition_cols=['filename','index_trip_start_in_clean_data'])
executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 3 Read, analyze and summarize GTFS Data : {executionTime}")
print("*"*100)

