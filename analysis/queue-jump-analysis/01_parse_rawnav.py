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
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")

# 1 Import Libraries and Set Global Parameters
########################################################################################################################

# 1.1 Import Python Libraries
############################################
from datetime import datetime

begin_time = datetime.now()  ##
print("Begin Time : {}".format(begin_time))
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

# List of Routes 
q_jump_route_list = ['S1', 'S2', 'S4', 'S9', 
                     '70', '79', 
                     '64', 'G8', 
                     'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
# If desired, a subset of routes above or the entire list. Code will iterate on the analysis_routes list
analysis_routes = q_jump_route_list

run_inventory = False # inventory (or re-inventory files), otherwise reload saved inventory if available
run_existing = False # whether to redo outputs that currently exist or skip over them

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

execution_time = str(datetime.now() - begin_time).split('.')[0]
print("Run Time Section 1 Import Libraries and Set Global Parameters : {}".format(execution_time))

# 2 Identify Relevant Files for Analysis Routes
########################################################################################################################
begin_time = datetime.now()  ##

# Create a list of zipped rawnavfiles (ala 'rawnav06544171027.txt.zip') as 
# file_universe. 
zipped_files_dir_parent = os.path.join(path_source_data, "October 2019 Rawnav")
file_universe = glob.glob(os.path.join(zipped_files_dir_parent, "*.txt.zip"))

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
        index = False
    )
       
else:
    try:
        rawnav_inventory = (
            pd.read_parquet(path=os.path.join(path_processed_data,"rawnav_inventory.parquet"))
            .assign(filename = lambda x: x.filename.astype(str)) #returned as categorical
        )
        
    except:
        raise("No rawnav inventory found")
 
rawnav_inventory_filtered = (
    rawnav_inventory[
        rawnav_inventory
        .groupby('filename',sort = False)['route']
        .transform(lambda x: x.isin(analysis_routes).any())
    ]
)
   
rawnav_inventory_filtered = (
    rawnav_inventory_filtered
    .assign(line_num = lambda x: x.line_num.astype('int'))
)
    
if len(rawnav_inventory_filtered) == 0:
    raise Exception("No Analysis Routes found in file_universe")

execution_time = str(datetime.now() - begin_time).split('.')[0]
print("Run Time Section 2 Identify Relevant Files for Analysis Routes : {}".format(execution_time))

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
    tag_info_line_no = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    reference = min(tag_info_line_no.line_num)
    # -1 refers to the fact that the tag line identifying the start of a run will be removed, such
    # that the second row associated with a run will become the first row of data. This helps to 
    # ensure that indices of the processed data will line up with values in the rawnav inventory
    tag_info_line_no.loc[:, "new_line_no"] = tag_info_line_no.line_num - reference - 1
    temp = wr.load_rawnav_data(
        zip_folder_path=row['fullpath'],
        skiprows=row['line_num'])

    if type(temp) != type(None):
        route_rawnav_tag_dict[row['filename']] = dict(RawData=temp, tagLineInfo=tag_info_line_no)
    else:
        remove_file = row['filename']  
        rawnav_inventory_filtered_valid  = rawnav_inventory_filtered_valid.query('filename!= @remove_file')

execution_time = str(datetime.now() - begin_time).split('.')[0]
print("Run Time Section 3 Load Raw RawNav Data : {}".format(execution_time))

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
print("Run Time Section 4 Clean RawNav Data : {}".format(execution_time))

# 5 Output
########################################################################################################################
begin_time = datetime.now()  
# 5.1 Output Summary Rawnav data
############################################

# Combine summary files, filter to analysis routes, convert col types once NAs removed
summary_rawnav = pd.concat(summary_data_dict.values())

summary_rawnav = summary_rawnav[summary_rawnav['route'].isin(analysis_routes)]
summary_rawnav = summary_rawnav.assign(
    route=lambda x: x.route.astype('str'),
    pattern=lambda x: x.pattern.astype('int32'))

# Remove duplicate runs
summary_rawnav = summary_rawnav[
    ~summary_rawnav.duplicated(['filename', 'index_run_start'], keep='last')]  

# Output Summary Files
path_summary_rawnav = os.path.join(path_processed_data,"rawnav_summary.parquet")

if not os.path.isdir(path_summary_rawnav):
    os.mkdir(path_summary_rawnav)
      
for analysis_route in analysis_routes:
    
    path_summary_route = os.path.join(path_summary_rawnav,"route={}".format(analysis_route))
    
    if (os.path.isdir(path_summary_route) and run_existing) or (not os.path.isdir(path_summary_route)): 
        shutil.rmtree(os.path.join(path_summary_route), ignore_errors=True) 
           
        summary_rawnav_fil = summary_rawnav.query('route == @analysis_route')
        
        table_summary = pa.Table.from_pandas(summary_rawnav_fil,
                                             schema = wr.rawnav_summary_schema())
        
        pq.write_to_dataset(table_summary, 
                            root_path=os.path.join(path_summary_rawnav),
                            partition_cols=['route','wday'])
    else:
        print('skipping summary output of {}'.format(analysis_route))
    
# 5.2 Output Processed Rawnav data
############################################
# Export Data
path_rawnav_data = os.path.join(path_processed_data, "rawnav_data.parquet")

if not os.path.isdir(path_rawnav_data):
    os.mkdir(path_rawnav_data)

for analysis_route in analysis_routes:
    
    path_rawnav_route = os.path.join(path_rawnav_data,"route={}".format(analysis_route))
    
    if (os.path.isdir(path_rawnav_route) and run_existing) or (not os.path.isdir(path_rawnav_route)): 
        shutil.rmtree(os.path.join(path_rawnav_route), ignore_errors=True) 
    
        # Merge Cleaned Rawnav Files Containing The Analysis Route
        out_rawnav_dat = wr.subset_rawnav_run(
            rawnav_data_dict_=rawnav_data_dict,
            rawnav_inventory_filtered_valid_=rawnav_inventory_filtered_valid,
            analysis_routes_=analysis_route)
        
        if out_rawnav_dat.shape == (0, 0):
            continue
        
        # Join Additional Identifying Information
        temp = summary_rawnav.query('route == @analysis_route')\
            [['filename', 'index_run_start', 'wday', 'start_date_time']]
                
        out_rawnav_dat = out_rawnav_dat.merge(temp, 
                                              on=['filename', 'index_run_start'], 
                                              how='left')
    
        assert (out_rawnav_dat.groupby(['filename', 'index_run_start', 'index_loc'])['index_loc'].
                count().values.max() == 1)
        
        # Column conversion after missing values removed
        out_rawnav_dat = out_rawnav_dat.assign(
            route=lambda x: x.route.astype('str'),
            #should be okay as int32 if everything goes to plan, but for safety will keep as double
            # and convert pattern later
            pattern=lambda x: x.pattern.astype('double')) 
    
        # Output    
        shutil.rmtree(os.path.join(path_rawnav_data,"route={}".format(analysis_route)), ignore_errors=True) 
                
        table = pa.Table.from_pandas(out_rawnav_dat,
                                     schema = wr.rawnav_data_schema())
        
        pq.write_to_dataset(table, 
                            root_path=os.path.join(path_rawnav_data),
                            partition_cols=['route','wday'])
    else:
        print('skipping output of {}'.format(analysis_route))

execution_time = str(datetime.now() - begin_time).split('.')[0]
print("Run Time Section 5 Output : {}".format(execution_time))
end_time = datetime.now()
print("End Time : {}".format(end_time))
########################################################################################################################
########################################################################################################################
