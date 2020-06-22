# -*- coding: utf-8 -*-
"""
Create by: abibeka
Purpose: check linestring issue and distance to nearest_stop issues
Created on 6/20/2020
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
from shapely.geometry import LineString
import shapely.wkt
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
                     'H8', 'W47']  # 16 Gb RAM can't handle all these at one go
analysis_routes = q_jump_route_list

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")
print("*" * 100)

analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

list_dat = []
nearest_rawnav_to_wmata_schedule_file = 'C:\\Users\\abibeka\\OneDrive - Kittelson & Associates, Inc\\Documents\\' \
                                        'WMATA-AVL\\Data\\ProcessedData\\wmata_schedule_based_sum_dat\\W47\\Monday' \
                                        '\\wmata_schedule_stop_locations_inventory-W47_Monday.xlsx'

for analysis_route in analysis_routes:
    for analysis_day in analysis_days:
        try:
            nearest_rawnav_to_wmata_schedule_file = \
                os.path.join(path_processed_data, 'wmata_schedule_based_sum_dat', analysis_route, analysis_day,
                             f'wmata_schedule_stop_locations_inventory-{analysis_route}_{analysis_day}.xlsx')
            dat = pd.read_csv(nearest_rawnav_to_wmata_schedule_file)
            list_dat.append(dat)
        except:
            print(f'route {analysis_route} does not have data on {analysis_day}')

nearest_rawnav_to_wmata_schedule_dat = pd.concat(list_dat)
nearest_rawnav_to_wmata_schedule_dat.reset_index(inplace=True)
nearest_rawnav_to_wmata_schedule_dat.geometry = \
    nearest_rawnav_to_wmata_schedule_dat.geometry.apply(lambda x: shapely.wkt.loads(x))

import numpy as np
nearest_rawnav_to_wmata_schedule_dat.loc[:,'coordinates_geometry']=\
    nearest_rawnav_to_wmata_schedule_dat.geometry.apply(lambda x: np.ravel(x.coords))

# Check if the goemetry always have 4 values in it:
nearest_rawnav_to_wmata_schedule_dat.coordinates_geometry.str.len().describe()
# count    1302292.0
# mean           4.0
# std            0.0
# min            4.0
# 25%            4.0
# 50%            4.0
# 75%            4.0
# max            4.0
# Name: coordinates_geometry, dtype: float64

# Looks good
