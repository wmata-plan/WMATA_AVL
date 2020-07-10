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
ipython.magic("autoreload")

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
# In general, set analysis_routes to the largest set of routes you are likely to look at --
# the list of routes can be subset further later.
restrict_n = None
# analysis_routes = ['W47'] 
analysis_routes = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4', 
                    'H8', 'W47'] 
zip_parent_folder_name = "October 2019 Rawnav"
# Assumes directory structure:
# zip_parent_folder_name (e.g, October 2019 Rawnav)
#  -- zipped_files_dirs (e.g., Vehicles 0-2999.zip)
#     -- file_universe (items in various zipped_files_dirs ala rawnav##########.txt.zip

run_inventory = True # inventory (or re-inventory files), otherwise reload saved inventory if available

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

file_universe = wr.get_zipped_files_from_zip_dir(
    zip_dir_list=zipped_files_dirs,
    zipped_files_dir_parent=zipped_files_dir_parent,
    glob_search="*.zip")

if run_inventory: 
    rawnav_inventory = wr.find_rawnav_routes(file_universe, nmax=restrict_n, quiet=True)
    
    path_rawnav_inventory = os.path.join(path_processed_data,"rawnav_inventory.parquet")
    shutil.rmtree(path_rawnav_inventory, ignore_errors=True) 
    os.mkdir(path_rawnav_inventory)
        
    # Note: partitioning required, using filename avoids resorting of values, filename column
    # will be sorted to end on reload however.
    rawnav_inventory.to_parquet(
        path = path_rawnav_inventory,
        partition_cols = ['filename'],
        index = False) 
       
else:
    try:
        rawnav_inventory = (
            pd.read_parquet(path=os.path.join(path_processed_data,"rawnav_inventory.parquet"))
            .assign(filename = lambda x: x.filename.astype(str)) #returned as categorical
            )
        
    except:
        raise("No rawnav inventory found")
 
rawnav_inventory_filtered =\
    rawnav_inventory[rawnav_inventory.groupby('filename',sort = False)['route'].transform(lambda x: x.isin(analysis_routes).any())]
   
rawnav_inventory_filtered = rawnav_inventory_filtered.assign(line_num = lambda x: x.line_num.astype('int'))
    
if len(rawnav_inventory_filtered) == 0:
    raise Exception("No Analysis Routes found in file_universe")

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Identify Relevant Files for Analysis Routes : {execution_time}")

# 3 Load Raw RawNav data
########################################################################################################################
begin_time = datetime.now()
# data is loaded into a dictionary named by the ID
route_rawnav_tag_dict = {}

# Iterate over each file, skipping to the first row where data in our filtered inventory is found
# Rather than read run-by-run, we read the rest of the file, then filter to relevant routes later
rawnav_inv_filt_first = rawnav_inventory_filtered.groupby(['fullpath', 'filename']).line_num.min().reset_index()
rawnav_inventory_filtered_valid = rawnav_inventory_filtered

for index, row in rawnav_inv_filt_first.iterrows():
    # TODO: I don't quite get the tag_line_info_no bit
    tag_info_line_no = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    reference = min(tag_info_line_no.line_num)
    tag_info_line_no.loc[:, "new_line_no"] = tag_info_line_no.line_num - reference - 1
    temp = wr.load_rawnav_data(
        zip_folder_path=row['fullpath'],
        skiprows=row['line_num'])

    if type(temp) != type(None):
        route_rawnav_tag_dict[row['filename']] = dict(RawData=temp, tagLineInfo=tag_info_line_no)
    else:
        remove_file = row['filename']  # remove bad read files
        rawnav_inventory_filtered_valid  = rawnav_inventory_filtered_valid.query('filename!= @remove_file')

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
begin_time = datetime.now()  
# 5.1 Output Summary Rawnav data
############################################

# Combine summary files, filter to analysis routes, convert col types
summary_rawnav = pd.concat(summary_data_dict.values())

summary_rawnav = summary_rawnav[summary_rawnav['route'].isin(analysis_routes)]
summary_rawnav = summary_rawnav.assign(
    route=lambda x: x.route.astype('str'),
    pattern=lambda x: x.pattern.astype('int32'))

# Remove duplicate runs
summary_rawnav = summary_rawnav[
    ~summary_rawnav.duplicated(['filename', 'index_run_start'], keep='last')]  

# Output Summary Files
path_summary_rawnav = os.path.join(path_processed_data,"summary_rawnav.parquet")
shutil.rmtree(path_summary_rawnav, ignore_errors=True) 
os.mkdir(path_summary_rawnav)

summary_rawnav.to_parquet(
    path = path_summary_rawnav,
    partition_cols = ['route'],
    index = False)

# 5.2 Output Processed Rawnav data
############################################
for analysis_route in analysis_routes:
    
    # Merge Cleaned Rawnav Files Containing The Analysis Route
    out_rawnav_dat = wr.subset_rawnav_run(
        rawnav_data_dict_=rawnav_data_dict,
        rawnav_inventory_filtered_valid_=rawnav_inventory_filtered_valid,
        analysis_routes_=analysis_route)
    
    if out_rawnav_dat.shape == (0, 0):
        continue
    
    # Filter to Runs and Join Additional Identifying Information
    temp = summary_rawnav[['filename', 'index_run_start', 'wday', 'start_date_time']]
    out_rawnav_dat = out_rawnav_dat.merge(temp, 
                                          on=['filename', 'index_run_start'], 
                                          how='right')

    assert (out_rawnav_dat.groupby(['filename', 'index_run_start', 'index_loc'])['index_loc'].
            count().values.max() == 1)
    
    # Export Data
    path_rawnav_data = os.path.join(path_processed_data, "rawnav_data.parquet")
    shutil.rmtree(path_rawnav_data, ignore_errors=True)  
    os.mkdir(path_rawnav_data) 
    
    pq.write_to_dataset(pa.Table.from_pandas(out_rawnav_dat), 
                        root_path=os.path.join(path_rawnav_data),
                        partition_cols=['route','wday'])

execution_time = str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 5 Output : {execution_time}")
end_time = datetime.now()
print(f"End Time : {end_time}")
########################################################################################################################
########################################################################################################################
