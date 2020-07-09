# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Purpose: Process rawnav data and output summary and processed dataset.
Created on: Thu Apr  2 12:35:10 2020
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

begin_time = datetime.now()  ##
print(f"Begin Time : {begin_time}")
import pandas as pd, os, sys, glob, shutil
import pyarrow as pa
import pyarrow.parquet as pq

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
    path_processed_data = os.path.join(path_sp, "data", "02-processed")
elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    gtfs_dir = os.path.join(path_source_data, "google_transit")
    # Processed data
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")
# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None 
restrict_n = 500
# analysis_routes = ['S9','70','79'] # Ran
# analysis_routes = ['S1','S2','S4','64'] # Ran
# analysis_routes = ['G8','D32','H1','H2','H3','H4'] #Ran
# analysis_routes = ['H8', 'W47'] #Ran
analysis_routes = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4', 'H8',
                   'W47']  # 16 Gb RAM
# can't handle all these at one go
zip_parent_folder_name = "October 2019 Rawnav"
# Assumes directory structure:
# zip_parent_folder_name (e.g, October 2019 Rawnav)
#  -- zipped_files_dirs (e.g., Vehicles 0-2999.zip)
#     -- file_universe (items in various zipped_files_dirs ala rawnav##########.txt.zip

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {execution_time}")

# 2 Identify Relevant Files for Analysis Routes
########################################################################################################################
begin_time = datetime.now()  ##
# Extract parent zipped folder and get the zipped files path
zipped_files_dir_parent = os.path.join(path_source_data, zip_parent_folder_name)
zipped_files_dirs = glob.glob(os.path.join(zipped_files_dir_parent, 'Vehicles *.zip'))
# Can use the unzipped files directly:
# un_zipped_files_dir =  glob.glob(os.path.join(path_source_data, zipped_files_dir_parent, 'Vehicles*[0-9]'))
file_universe = wr.get_zipped_files_from_zip_dir(
    zip_dir_list=zipped_files_dirs,
    zipped_files_dir_parent=zipped_files_dir_parent,
    glob_search="*.zip")
# Return a dataframe of routes and details
rawnav_inventory = wr.find_rawnav_routes(file_universe, nmax=restrict_n, quiet=True)
# TODO : Get the file Universe for all files in one run and Store this file_universe. Might save 50 min.
# Filter to any file including at least one of our analysis routes 
# Note that other non-analysis routes will be included here, but information about these routes
# is currently necessary to split the rawnav file correctly. 
rawnav_inventory_filtered = \
    rawnav_inventory[rawnav_inventory.groupby('filename')['route'].transform(lambda x: x.isin(analysis_routes).any())]
# Now that NAs have been removed from files without data, we can convert this to an integer type
rawnav_inventory_filtered['line_num'] = rawnav_inventory_filtered.line_num.astype('int')
# Having Retrieve tag information at file level. Need tags from other routes to define a trip. 
# Will subset data by route later
if len(rawnav_inventory_filtered) == 0:
    raise Exception("No Analysis Routes found in file_universe")
# Return filtered list of files to pass to read-in functions, starting
# with first rows
rawnav_inv_filt_first = rawnav_inventory_filtered.groupby(['fullpath', 'filename']).line_num.min().reset_index()

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Identify Relevant Files for Analysis Routes : {execution_time}")
# 3 Load Raw RawNav data
########################################################################################################################
begin_time = datetime.now()
# data is loaded into a dictionary named by the ID
route_rawnav_tag_dict = {}
for index, row in rawnav_inv_filt_first.iterrows():
    tag_info_line_no = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    tag_info_line_no.line_num = tag_info_line_no.line_num.astype(int)
    reference = min(tag_info_line_no.line_num)
    tag_info_line_no.loc[:, "NewLineNo"] = tag_info_line_no.line_num - reference - 1
    # FileID gets messy; string to number conversion loose the initial zeros. "filename" is easier to deal with.
    temp = wr.load_rawnav_data(
        zip_folder_path=row['fullpath'],
        skiprows=row['line_num'])
    if type(temp) != type(None):
        route_rawnav_tag_dict[row['filename']] = dict(RawData=temp, tagLineInfo=tag_info_line_no)
    else:
        remove_file = row['filename']  # remove bad read files
        rawnav_inventory_filtered = rawnav_inventory_filtered.query('filename!= @remove_file')
        rawnav_inv_filt_first = rawnav_inv_filt_first.query('filename!= @remove_file')

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 3 Load Raw RawNav Data : {execution_time}")
# 4 Clean RawNav data
########################################################################################################################
begin_time = datetime.now()  ##
rawnav_data_dict = {}
summary_data_dict = {}
for key, datadict in route_rawnav_tag_dict.items():
    temp_dat = wr.clean_rawnav_data(
        data_dict=datadict,
        filename=key)
    rawnav_data_dict[key] = temp_dat['rawnavdata']
    summary_data_dict[key] = temp_dat['summary_data']
route_rawnav_tag_dict = None

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 4 Clean RawNav Data : {execution_time}")
# 5 Output
########################################################################################################################
begin_time = datetime.now()  ##
# 5.1 Output Summary data
############################################
fin_summary_dat = pd.DataFrame()
fin_summary_dat = pd.concat(summary_data_dict.values())
fin_summary_dat.loc[:, "count1"] = fin_summary_dat.groupby(['filename', 'IndexTripStartInCleanData'])[
    'IndexTripStartInCleanData'].transform('count')
issue_dat = fin_summary_dat.query('count1>1')  # Some empty trips cause issue with pandas asof merge. Find these trips.
# Some empty trips cause issue with pandas asof merge. 2nd way to find these trips.
# issue_dat = fin_summary_dat.query('IndexTripStartInCleanData>IndexTripEnd')
fin_summary_dat = fin_summary_dat[
    ~fin_summary_dat.duplicated(['filename', 'IndexTripStartInCleanData'], keep='last')]  # Remove duplicate trips
# Output Summary Files
for analysis_route in analysis_routes:
    out_sum_dat = fin_summary_dat.query('route==@analysis_route')
    if not os.path.isdir(os.path.join(path_processed_data, "RouteData")): os.mkdir(
        os.path.join(path_processed_data, "RouteData"))
    out_sum_fi = os.path.join(path_processed_data, 'RouteData',
                              f'TripSummaries_Route{analysis_route}_Restrict{restrict_n}.csv')
    out_sum_dat.to_csv(out_sum_fi)

# 5.2 Output Processed data
############################################
for analysis_route in analysis_routes:
    out_rawnav_dat = wr.subset_rawnav_trip(
        rawnav_data_dict_=rawnav_data_dict,
        rawnav_inventory_filtered_=rawnav_inventory_filtered,
        analysis_routes_=analysis_route)
    if out_rawnav_dat.shape == (0, 0):
        continue
    # Check for duplicate IndexLoc
    assert (out_rawnav_dat.groupby(['filename', 'IndexTripStartInCleanData', 'IndexLoc'])['IndexLoc'].
            count().values.max() == 1)
    temp = fin_summary_dat[['filename', 'IndexTripStartInCleanData', 'wday', 'StartDateTime']]
    out_rawnav_dat = out_rawnav_dat.merge(temp, on=['filename', 'IndexTripStartInCleanData'], how='left')
    out_rawnav_dat = out_rawnav_dat.assign(Lat=lambda x: x.Lat.astype('float'),
                                           Heading=lambda x: x.Heading.astype('float'),
                                           IndexTripStartInCleanData=lambda x: x.IndexTripStartInCleanData.astype(
                                               'int'),
                                           IndexTripEndInCleanData=lambda x: x.IndexTripEndInCleanData.astype('int'))
    assert (out_rawnav_dat.groupby(['filename', 'IndexTripStartInCleanData', 'IndexLoc'])['IndexLoc'].
            count().values.max() == 1)
    table_from_pandas = pa.Table.from_pandas(out_rawnav_dat)
    if not os.path.isdir(os.path.join(path_processed_data, "RouteData")): os.mkdir(
        os.path.join(path_processed_data, "RouteData"))
    remove_folder = \
        os.path.join(path_processed_data, 'RouteData', f"Route{analysis_route}_Restrict{restrict_n}.parquet")
    ## Try to delete the file ##
    while os.path.isdir(remove_folder):
        shutil.rmtree(remove_folder, ignore_errors=True)  # Remove data from remove_folder before writing
    pq.write_to_dataset(table_from_pandas, root_path=os.path.join(remove_folder), \
                        partition_cols=['wday'])

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 5 Output : {execution_time}")
end_time = datetime.now()
print(f"End Time : {end_time}")
########################################################################################################################
########################################################################################################################
