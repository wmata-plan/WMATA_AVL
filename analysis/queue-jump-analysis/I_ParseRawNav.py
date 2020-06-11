# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Purpose: Process rawnav data and output summary and processed dataset.
Created on: Thu Apr  2 12:35:10 2020
"""
# 0 Housekeeping. Clear variable space
###########################################################################################################################################################
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

# 1 Import Libraries and Set Global Parameters
###########################################################################################################################################################
# 1.1 Import Python Libraries
############################################
from datetime import datetime
begin_time = datetime.now() ##
print(f"Begin Time : {begin_time}")
import pandas as pd, os, sys, glob, shutil
import pyarrow as pa
import pyarrow.parquet as pq
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore") #Stop Pandas warnings

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
# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None 
restrict_n = 1000
#AnalysisRoutes = ['S9','70','79'] # Ran
# AnalysisRoutes = ['S1','S2','S4','64'] # Ran
#AnalysisRoutes = ['G8','D32','H1','H2','H3','H4'] #Ran
AnalysisRoutes = ['H8','W47'] #Ran
#AnalysisRoutes = ['S1','S2','S4','S9','70','79','64','G8','D32','H1','H2','H3','H4','H8','W47'] # 16 Gb RAM can't handle all these at one go
ZipParentFolderName = "October 2019 Rawnav"
# Assumes directory structure:
# ZipParentFolderName (e.g, October 2019 Rawnav)
#  -- ZippedFilesDirs (e.g., Vehicles 0-2999.zip)
#     -- FileUniverse (items in various ZippedFilesDirs ala rawnav##########.txt.zip

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")

#2 Identify Relevant Files for Analysis Routes
###########################################################################################################################################################
begin_time = datetime.now() ##
#Extract parent zipped folder and get the zipped files path
ZippedFilesDirParent = os.path.join(path_source_data, ZipParentFolderName)
ZippedFilesDirs = glob.glob(os.path.join(ZippedFilesDirParent,'Vehicles *.zip'))
#Can use the unzipped files directly:
#UnZippedFilesDir =  glob.glob(os.path.join(path_source_data,ZippedFilesDirParent,'Vehicles*[0-9]'))
FileUniverse = wr.GetZippedFilesFromZipDir(ZippedFilesDirs,ZippedFilesDirParent) 
# Return a dataframe of routes and details
rawnav_inventory = wr.find_rawnav_routes(FileUniverse, nmax = restrict_n, quiet = True)
# TODO : Get the File Universe for all files in one run and Store this FileUniverse. Might save 50 min.
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

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Identify Relevant Files for Analysis Routes : {executionTime}")
# 3 Load Raw RawNav Data
###########################################################################################################################################################
begin_time = datetime.now() ##
# Data is loaded into a dictionary named by the ID
RouteRawTagDict = {}
for index, row in rawnav_inv_filt_first.iterrows():
    tagInfo_LineNo = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    Refrence = min(tagInfo_LineNo.line_num)
    tagInfo_LineNo.loc[:,"NewLineNo"] = tagInfo_LineNo.line_num - Refrence-1
    # FileID gets messy; string to number conversion loose the initial zeros. "filename" is easier to deal with.
    temp = wr.load_rawnav_data(ZipFolderPath = row['fullpath'], skiprows = row['line_num'])
    if type(temp)!= type(None):
        RouteRawTagDict[row['filename']] = {'RawData':temp,'tagLineInfo':tagInfo_LineNo}
    else:
        removeFile= row['filename'] #remove bad read files
        rawnav_inventory_filtered = rawnav_inventory_filtered.query('filename!= @removeFile')
        rawnav_inv_filt_first = rawnav_inv_filt_first.query('filename!= @removeFile')

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 3 Load Raw RawNav Data : {executionTime}") 
# 4 Clean RawNav Data
###########################################################################################################################################################
begin_time = datetime.now() ##
RawnavDataDict = {}
SummaryDataDict = {}
for key, datadict in RouteRawTagDict.items():
    Temp = wr.clean_rawnav_data(datadict, key)
    RawnavDataDict[key] = Temp['rawnavdata']
    SummaryDataDict[key] = Temp['SummaryData']
RouteRawTagDict = None

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 4 Clean RawNav Data : {executionTime}") 
# 5 Output
###########################################################################################################################################################
begin_time = datetime.now() ##
# 5.1 Output Summary Data
############################################
FinSummaryDat = pd.DataFrame()
FinSummaryDat = pd.concat(SummaryDataDict.values()) 
FinSummaryDat.loc[:,"count1"]=FinSummaryDat.groupby(['filename','IndexTripStartInCleanData'])['IndexTripStartInCleanData'].transform('count')
IssueDat = FinSummaryDat.query('count1>1') #Some empty trips cause issue with pandas asof merge. Find these trips.
#Some empty trips cause issue with pandas asof merge. 2nd way to find these trips.
#IssueDat = FinSummaryDat.query('IndexTripStartInCleanData>IndexTripEnd')
FinSummaryDat = FinSummaryDat[~FinSummaryDat.duplicated(['filename','IndexTripStartInCleanData'],keep='last')] #Remove duplicate trips
#Output Summary Files
for AnalysisRoute in AnalysisRoutes:
    OutSumDat = FinSummaryDat.query('route==@AnalysisRoute')
    if not os.path.isdir(os.path.join(path_processed_data, "RouteData")): os.mkdir(os.path.join(path_processed_data, "RouteData"))
    OutFiSum = os.path.join(path_processed_data,'RouteData',f'TripSummaries_Route{AnalysisRoute}_Restrict{restrict_n}.csv')
    OutSumDat.to_csv(OutFiSum)

# 5.2 Output Processed Data
############################################
for AnalysisRoute in AnalysisRoutes:
    OutDat = wr.subset_rawnav_trip(RawnavDataDict, rawnav_inventory_filtered, AnalysisRoute)
    if OutDat.shape == (0,0):
        continue
    #Check for duplicate IndexLoc
    assert(OutDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)
    temp = FinSummaryDat[['filename','IndexTripStartInCleanData','wday','StartDateTime']]
    OutDat = OutDat.merge(temp, on = ['filename','IndexTripStartInCleanData'],how='left')
    OutDat = OutDat.assign(Lat = lambda x: x.Lat.astype('float'),
                               Heading = lambda x: x.Heading.astype('float'),
                               IndexTripStartInCleanData =lambda x: x.IndexTripStartInCleanData.astype('int'),
                               IndexTripEndInCleanData =lambda x: x.IndexTripEndInCleanData.astype('int'))
    assert(OutDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)
    table_from_pandas = pa.Table.from_pandas(OutDat)
    if not os.path.isdir(os.path.join(path_processed_data, "RouteData")): os.mkdir(os.path.join(path_processed_data, "RouteData"))
    RemFolder = os.path.join(path_processed_data,'RouteData',f"Route{AnalysisRoute}_Restrict{restrict_n}.parquet")
    ## Try to delete the file ##
    while os.path.isdir(RemFolder):
        shutil.rmtree (RemFolder, ignore_errors=True) #Remove data from RemFolder before writing
    pq.write_to_dataset(table_from_pandas,root_path =os.path.join(RemFolder),\
    partition_cols=['wday'])
        
executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 5 Output : {executionTime}") 
end_time = datetime.now()
print(f"End Time : {end_time}")
###########################################################################################################################################################
###########################################################################################################################################################