# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 12:35:10 2020

"""
#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

#1 Import Libraries
########################################################################################
import pandas as pd, os, numpy as np, pyproj, sys, zipfile, glob, logging
from datetime import datetime
from geopy.distance import geodesic
from collections import defaultdict
from shapely.geometry import Point
import pyarrow as pa
import pyarrow.parquet as pq
import geopandas as gpd
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore") #Too many Pandas warnings

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Queue Jump Analysis"
    
    # Source Data
    # path_source_data = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\102019 sample")
    path_source_data = r"C:\Downloads"
    GTFS_Dir = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\wmata-2019-05-18 dl20200205gtfs")

    # Processed Data
    path_processed_data = os.path.join(path_sp,r"Client Shared Folder\data\02-processed")
elif os.getlogin()=="abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working) 
    
    # Source Data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    GTFS_Dir = os.path.join(path_source_data, "google_transit")   
    # Processed Data
    path_processed_data = os.path.join(path_source_data,"ProcessedData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, GTFS_Dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

# User-Defined Package
import wmatarawnav as wr

# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None 
restrict_n = None
AnalysisRoutes = ['79']
ZipParentFolderName = "October 2019 Rawnav"
# Assumes directory structure:
# ZipParentFolderName (e.g, October 2019 Rawnav)
#  -- ZippedFilesDirs (e.g., Vehicles 0-2999.zip)
#     -- FileUniverse (items in various ZippedFilesDirs ala rawnav##########.txt.zip

#2 Indentify Relevant Files for Analysis Routes
########################################################################################
#Extract parent zipped folder and get the zipped files path
ZippedFilesDirParent = os.path.join(path_source_data, ZipParentFolderName)
ZippedFilesDirs = glob.glob(os.path.join(path_source_data,ZipParentFolderName,'Vehicles *.zip'))
UnZippedFilesDir =  glob.glob(os.path.join(path_source_data,ZippedFilesDirParent,'Vehicles*[0-9]'))

FileUniverse = wr.GetZippedFilesFromZipDir(ZippedFilesDirs,ZippedFilesDirParent) 
# Return a dataframe of routes and details
rawnav_inventory = wr.find_rawnav_routes(FileUniverse, nmax = restrict_n, quiet = True)
# Filter to any file including at least one of our analysis routes 
# Note that other non-analysis routes will be included here, but information about these routes
# is currently necessary to split the rawnav file correctly. 
rawnav_inventory_filtered = rawnav_inventory[rawnav_inventory.groupby('filename')['route'].transform(lambda x: x.isin(AnalysisRoutes).any())]
# Now that NAs have been removed from files without data, we can convert this to an integer type
rawnav_inventory_filtered['line_num'] = rawnav_inventory_filtered.line_num.astype('int')
# Having Retrieve tag information at file level. Need tags from other routes to define a trip. 
# Will subset data by route later
if (len(rawnav_inventory_filtered) ==0):
    raise Exception ("No Analysis Routes found in FileUniverse")
# Return filtered list of files to pass to read-in functions, starting
# with first rows
rawnav_inv_filt_first = rawnav_inventory_filtered.groupby(['fullpath','filename']).line_num.min().reset_index()

# 3 Load Raw RawNav Data
########################################################################################
# Data is loaded into a dictionary named by the ID
RouteRawTagDict = {}

for index, row in rawnav_inv_filt_first.iterrows():
    tagInfo_LineNo = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    Refrence = min(tagInfo_LineNo.line_num)
    tagInfo_LineNo.loc[:,"NewLineNo"] = tagInfo_LineNo.line_num - Refrence-1
    # FileID gets messy; string to number conversion loose the initial zeros. "filename" is easier to deal with.
    temp = wr.load_rawnav_data(ZipFolderPath = row['fullpath'], skiprows = row['line_num'])
    RouteRawTagDict[row['filename']] = {'RawData':temp,'tagLineInfo':tagInfo_LineNo}
    
# 4 Clean RawNav Data
########################################################################################
#CleanDataDict = {}
RawnavDataDict = {}
SummaryDataDict = {}

# DataDict = RouteRawTagDict['rawnav02683191011.txt']
for key, datadict in RouteRawTagDict.items():
    #CleanDataDict[key] = wr.clean_rawnav_data(datadict)
    Temp = wr.clean_rawnav_data(datadict, key)
    RawnavDataDict[key] = Temp['rawnavdata']
    SummaryDataDict[key] = Temp['SummaryData']
RouteRawTagDict = None

# 5 Output
########################################################################################
FinSummaryDat = pd.DataFrame()
FinSummaryDat = pd.concat(SummaryDataDict.values()) # 
FinSummaryDat.loc[:,"count1"]=FinSummaryDat.groupby(['filename','IndexTripStartInCleanData'])['IndexTripStartInCleanData'].transform('count')
IssueDat = FinSummaryDat.query('count1>1')
FinSummaryDat = FinSummaryDat[~FinSummaryDat.duplicated(['filename','IndexTripStartInCleanData'],keep='last')] 
#These two keys should have given unique trips but there was an issue with one of the file. 
#Look at IssueDat to get more information on this issue. Duplicates occur when two tags occur 
# with no data in between thus both these tags would have the same IndexTripStartInCleanData. 
#Check Line 3842 and Line 3848 of file "rawnav06435191012.txt" to see this issue:
"""
   7901,6435,10/11/19,09:15:22,44999,05280
/ 09:15:22 Buswares is Shutting down WaitResult SHUTDOWN,00000
/ 09:18:09 BWRawNav Collection Module was STARTED 10.1.0.172 DB=S1000081a_SBA
cal,0,0
cal,205403,653
/ 09:18:46 Buswares is now using route zero 
   7901,6435,10/11/19,09:18:47,44999,05280
38.994298,-77.030058,312,C,S,000000,0003,09,   ,9,38.994298,-77.030058
"""
#We only need the 2nd Tag as that giver the correct info.

#Output Summary Files
OutFiSum = os.path.join(path_processed_data,'TripSummaries.csv')
FinSummaryDat.to_csv(OutFiSum)

########################
FinDat = wr.subset_rawnav_trip1(RawnavDataDict, rawnav_inventory_filtered, AnalysisRoutes)
#Check for duplicate IndexLoc
assert(FinDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)
temp = FinSummaryDat[['filename','IndexTripStartInCleanData','wday','StartDateTime']]
FinDat = FinDat.merge(temp, on = ['filename','IndexTripStartInCleanData'],how='left')
FinDat = FinDat.assign(Lat = lambda x: x.Lat.astype('float'),
                           Heading = lambda x: x.Heading.astype('float'),
                           IndexTripStartInCleanData =lambda x: x.IndexTripStartInCleanData.astype('int'),
                           IndexTripEndInCleanData =lambda x: x.IndexTripEndInCleanData.astype('int'))
#RawnavDataDict = None
assert(FinDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)
table_from_pandas = pa.Table.from_pandas(FinDat)
#FinDat = None
## Get input ##
RemFolder = os.path.join(path_processed_data,"Route79_Partition.parquet")
# OverwritePrevData= input(f"Do you want to overwrite previous data in {RemFolder} (Y/N)?")
## Try to delete the file ##
try:
    os.remove(RemFolder)
except OSError as e:  ## if failed, report it back to the user ##
    print ("Error: %s - %s." % (e.filename, e.strerror))
#Remove data from RemFolder before writing
# pq.write_to_dataset(table_from_pandas,root_path =os.path.join(path_processed_data,"Route79_Partition.parquet"))
pq.write_to_dataset(table_from_pandas,root_path =os.path.join(path_processed_data,"Route79_Partition.parquet"),\
partition_cols=['wday','route'])


FinDat = pq.read_table(source =os.path.join(path_processed_data,"Route79_Partition.parquet")).to_pandas()

#Check for duplicate IndexLoc
assert(FinDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)

