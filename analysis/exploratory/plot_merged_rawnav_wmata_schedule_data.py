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
import pandas as pd, os, sys, collections
import shapely.wkt

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
restrict_n = None
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                     'H8','W47']  # 16 Gb RAM can't handle all these at one go
# analysis_routes = ['70', '64', 'D32', 'H8', 'S2']
# analysis_routes = ['S1', 'S9', 'H4', 'G8', '64']
#analysis_routes = ['S2','S4','H1','H2','H3','79','W47']
analysis_routes = ['H8']
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr
executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")
print("*"*100)


# 2.1 Rawnav data
############################################
rawnav_dat = wr.read_processed_rawnav(
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
rawnav_summary_keys_col = rawnav_summary_dat[['filename','index_trip_start_in_clean_data']]

# 2.3 Merge Processed and Summary data
############################################
rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col,on=['filename','index_trip_start_in_clean_data'],how='right')
rawnav_qjump_dat.pattern = rawnav_qjump_dat.pattern.astype('int')
# Having issues with route "70" and "64"---Getting read as int instead of str
rawnav_qjump_dat.route = rawnav_qjump_dat.route.astype(str)
rawnav_summary_dat.route = rawnav_summary_dat.route.astype(str)

set(rawnav_qjump_dat.index_trip_start_in_clean_data.unique()) -set(rawnav_summary_dat.index_trip_start_in_clean_data.unique())
executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Analyze Route ---Subset RawNav Data : {executionTime}")
print("*"*100)




list_dat = []
list_summary_dat = []

for analysis_route in analysis_routes:
    for analysis_day in analysis_days:
        try:
            nearest_rawnav_to_wmata_schedule_file = \
                os.path.join(path_processed_data, 'wmata_schedule_based_sum_dat', analysis_route, analysis_day,
                             f'wmata_schedule_stop_locations_inventory-{analysis_route}_{analysis_day}.xlsx')
            dat = pd.read_excel(nearest_rawnav_to_wmata_schedule_file,index=True)
            list_dat.append(dat)

            wmata_schedule_based_sum_file = \
                os.path.join(path_processed_data, 'wmata_schedule_based_sum_dat', analysis_route, analysis_day,
                             f'wmata_schedule_trip_summaries-{analysis_route}_{analysis_day}.xlsx')
            sum_dat = pd.read_excel(wmata_schedule_based_sum_file,index=True)
            list_summary_dat.append(sum_dat)
        except:
            print(f'route {analysis_route} does not have data on {analysis_day}')


nearest_rawnav_wmata_schedule_all_routes_days = pd.concat(list_dat)
nearest_rawnav_wmata_schedule_all_routes_days.reset_index(inplace=True)
nearest_rawnav_wmata_schedule_all_routes_days.geometry = \
    nearest_rawnav_wmata_schedule_all_routes_days.geometry.apply(lambda x: shapely.wkt.loads(x))

wmata_schedule_based_sum_dat_all_routes_days= pd.concat(list_summary_dat)

# 4 Plot Rawnav Trace and Nearest Stops
###########################################################################################################################################################
print(f"Run Section 4 Plot Rawnav Trace and Nearest Stops...")
begin_time = datetime.now()  ##
# 4.1 Add Summary data to Stop data from Plotting
############################################
subset_wmata_schedule_rawnav_dat = \
    nearest_rawnav_wmata_schedule_all_routes_days[['filename', 'index_trip_start_in_clean_data',
                                                                   'route_key','dist_from_previous_stop',
                                                                   'pattern_name', 'stop_sort_order', 'index_loc',
                                                                   'stop_lat', 'stop_lon','stop_heading',
                                                                   'dist_nearest_point_from_stop', 'geometry',
                                                                   'geo_description']]
wmata_schedule_based_sum_dat_all_routes_days.reset_index(inplace=True)
subset_wmata_schedule_rawnav_dat = \
    subset_wmata_schedule_rawnav_dat.merge(wmata_schedule_based_sum_dat_all_routes_days,
                                           on=['filename', 'index_trip_start_in_clean_data'],
                                           how='left')
subset_wmata_schedule_rawnav_dat = \
    subset_wmata_schedule_rawnav_dat[
        ['index_loc','filename', 'wday', 'start_date_time', 'end_date_time', 'route_pattern', 'route', 'pattern',
         'stop_sort_order','geo_description','route_text_wmata_schedule','pattern_name_wmata_schedule',
         'direction_wmata_schedule','pattern_destination_wmata_schedule',
         'direction_id_wmata_schedule','route_key','dist_from_previous_stop',
         'stop_heading','stop_lat', 'stop_lon', 'index_trip_start_in_clean_data',
         'index_trip_end_in_clean_data','dist_nearest_point_from_stop',
         'start_odom_ft_wmata_schedule', 'end_odom_ft_wmata_schedule',
         'trip_dist_mi_odom_and_wmata_schedule','trip_length_mi_direct_wmata_schedule',
         'dist_odom_mi', 'crow_fly_dist_lat_long_mi',
         'start_sec_wmata_schedule', 'end_sec_wmata_schedule',
         'trip_dur_sec_wmata_schedule', 'trip_dur_from_sec', 'trip_duration_from_tags',
         'start_lat_wmata_schedule', 'end_lat_wmata_schedule', 'start_long_wmata_schedule',
         'end_long_wmata_schedule', 'dist_first_stop_wmata_schedule',
         'trip_speed_mph_wmata_schedule', 'speed_odom_mph', 'speed_trip_tag_mph',
         'sec_start', 'odom_ft_start', 'sec_end',
         'odom_ft_end', 'lat_start', 'long_start', 'lat_end', 'long_end', 'geometry']]

subset_wmata_schedule_rawnav_dat.rename(columns={'index_loc': 'closest_index_loc_in_rawnav'}, inplace=True)

# 4.2 Plot Trajectories
############################################
subset_wmata_schedule_rawnav_dat.route = subset_wmata_schedule_rawnav_dat.route.astype('str')
rawnav_qjump_dat.route = rawnav_qjump_dat.route.astype('str')

group_rawnav_wmata_schedule = \
    subset_wmata_schedule_rawnav_dat.groupby(['filename', 'index_trip_start_in_clean_data', 'route'])
rawnav_qjump_dat = rawnav_qjump_dat.query("route in @analysis_routes")
rawnav_groups = rawnav_qjump_dat.groupby(['filename', 'index_trip_start_in_clean_data', 'route'])

tracker_usable_routes = collections.Counter()
for name, grp in group_rawnav_wmata_schedule:
    Pattern = grp["pattern"].values[0]
    tracker_usable_routes[f"{name[2]}_{Pattern}"] += 1
print(tracker_usable_routes)
STOP = 20
tracker = collections.Counter()

len(rawnav_groups.groups.keys())

for name, rawnav_grp in rawnav_groups:
    pattern = rawnav_grp["pattern"].values[0]
    if name in group_rawnav_wmata_schedule.groups:
        relevant_rawnav_wmata_schedule_dat = group_rawnav_wmata_schedule.get_group(name)
    else:
        print(name)
        continue
    tracker[f"{name[2]}_{pattern}"] += 1
    if tracker[f"{name[2]}_{pattern}"] >= STOP: continue
    print(name, pattern)
    wday = relevant_rawnav_wmata_schedule_dat["wday"].values[0]
    hour = \
        relevant_rawnav_wmata_schedule_dat.start_date_time.values[0].split(" ")[1].split(":")[0]
    save_file = f"{wday}_{hour}_{name[2]}_{pattern}_{name[0]}_Row{int(name[1])}.html"
    map1 = wr.plot_rawnav_trajectory_with_wmata_schedule_stops(
        rawnav_grp, relevant_rawnav_wmata_schedule_dat)
    save_dir_1 = os.path.join(path_processed_data, "TrajectoryFigures")
    if not os.path.exists(save_dir_1): os.makedirs(save_dir_1)
    save_dir_2 = os.path.join(save_dir_1, f'{name[2]}_{pattern}')
    if not os.path.exists(save_dir_2): os.makedirs(save_dir_2)
    save_dir_3 = os.path.join(path_processed_data, "TrajectoryFigures", f'{name[2]}_{pattern}', wday)
    if not os.path.exists(save_dir_3): os.makedirs(save_dir_3)
    map1.save(os.path.join(save_dir_3, f"{save_file}"))

executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 4 Plot Rawnav Trace and Nearest Stops : {executionTime}")
print("*" * 100)

###########################################################################################################################################################
