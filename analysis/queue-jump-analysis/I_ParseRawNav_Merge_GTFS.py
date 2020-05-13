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

#
ZippedFilesDirs =ZippedFilesDirs[-1]
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

for key, datadict in RouteRawTagDict.items():
    #CleanDataDict[key] = wr.clean_rawnav_data(datadict)
    Temp = wr.clean_rawnav_data(datadict, key)
    RawnavDataDict[key] = Temp['rawnavdata']
    SummaryDataDict[key] = Temp['SummaryData']
RouteRawTagDict = None
# TODO: Need to write processed data to database, HDF5, Feather, or parquet format.
    
# %%timeit -n 100
# FinSummaryDat = pd.DataFrame()
# for keys,data in SummaryDataDict.items():
#     FinSummaryDat = pd.concat([FinSummaryDat, data])    
# #Run Time: 57.1 ms ± 2.73 ms per loop (mean ± std. dev. of 7 runs, 100 loops each)

# %%timeit -n 100
# FinSummaryDat = pd.DataFrame()
# for keys,data in SummaryDataDict.items():
#     FinSummaryDat = FinSummaryDat.append(data)  
# #Run Time: 56.6 ms ± 2.94 ms per loop (mean ± std. dev. of 7 runs, 100 loops each)    
    
#%%timeit -n 100
FinSummaryDat = pd.DataFrame()
FinSummaryDat = pd.concat(SummaryDataDict.values()) # 
#Run Time: 14.7 ms ± 142 µs per loop (mean ± std. dev. of 7 runs, 100 loops each)
    
    
#Output Summary Files
now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H")
OutFiSum = os.path.join(path_processed_data,f'TripSummaries_{d4}.csv')
FinSummaryDat.to_csv(OutFiSum)

#5 Analyze Route 79---Subset RawNav Data. 
########################################################################################
# %%timeit -n 1
# FinDat1 = wr.subset_rawnav_trip(CleanDataDict, rawnav_inventory_filtered, AnalysisRoutes)
#1min 8s ± 2.04 s per loop (mean ± std. dev. of 7 runs, 1 loop each)

#%%timeit -n 5
FinDat = wr.subset_rawnav_trip1(RawnavDataDict, rawnav_inventory_filtered, AnalysisRoutes)
#1.93 s ± 86.9 ms per loop (mean ± std. dev. of 7 runs, 5 loops each)

SumDat, SumDatStart, SumDatEnd = wr.subset_summary_data(FinSummaryDat, AnalysisRoutes)

#6 Read the GTFS Data
########################################################################################
GtfsData = wr.readGTFS(GTFS_Dir)
FirstStopDat1_rte, CheckFirstStop, CheckFirstStop1 = wr.get1ststop(GtfsData,AnalysisRoutes)
LastStopDat1_rte, CheckLastStop, CheckLastStop1 = wr.getlaststop(GtfsData,AnalysisRoutes)
wr.debugGTFS1stLastStopData(CheckFirstStop,CheckFirstStop1,CheckLastStop,CheckLastStop1,path_processed_data)

#7 Analyze the Trip Start and End
#TODO: The code from section 8 can handle this part. Need to rewrite this
########################################################################################
SumDatStart, SumDatEnd = wr.getNearestStartEnd(SumDatStart, SumDatEnd, FirstStopDat1_rte, LastStopDat1_rte, AnalysisRoutes)

GeomNearest_start = FinDat.merge(SumDatStart,on =['filename','IndexTripStartInCleanData'],how='left')['nearest_start']
GeomNearest_end = FinDat.merge(SumDatEnd,on =['filename','IndexTripStartInCleanData'],how='left')['nearest_end']
geometryPoints = [Point(xy) for xy in zip(FinDat.Long.astype(float), FinDat.Lat.astype(float))]
FinDat.loc[:,'distances_start_ft'] = wr.GetDistanceLatLong_ft_fromGeom(GeomNearest_start, geometryPoints)
FinDat.loc[:,'distances_end_ft'] = wr.GetDistanceLatLong_ft_fromGeom(GeomNearest_end, geometryPoints)
SumDatWithGTFS = wr.GetSummaryGTFSdata(FinDat,SumDat)
SumDatWithGTFS.set_index(['fullpath','filename','file_id','wday','StartDateTime','EndDateTime','IndexTripStartInCleanData','taglist','route_pattern','route','pattern'],inplace=True,drop=True) 
#8 Output Summary Files
########################################################################################
now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H")
OutFiSum = os.path.join(path_processed_data,f'TripSummaries_{d4}.xlsx')
SumDatWithGTFS.to_excel(OutFiSum,merge_cells=False)

#8 Merge all stops to rawnav data
########################################################################################
GtfsData_UniqueStops = GtfsData[~GtfsData.duplicated(['route_id','direction_id','stop_name'],keep='first')]
NearestRawnavOnGTFS = wr.mergeStopsGTFSrawnav(GtfsData_UniqueStops, FinDat)
NearestRawnavOnGTFS = NearestRawnavOnGTFS[['filename','IndexTripStartInCleanData','direction_id'
                                           ,'stop_sequence','IndexLoc','route_id',
                                           'trip_headsign','stop_lat','stop_lon','stop_name'
                                           ,'Lat','Long','dist','geometry']]
NearestRawnavOnGTFS.sort_values(['filename','IndexTripStartInCleanData',
                                 'direction_id','stop_sequence'],inplace=True)

DatFirstStops = NearestRawnavOnGTFS.query("stop_sequence==1")
DatFirstStops.sort_values(['filename','IndexTripStartInCleanData',
                                 'direction_id','stop_sequence'],inplace=True)

# Will not work for routes like S9 and X0
DatFirstStops.loc[:,"GetDir"] = abs(DatFirstStops.IndexLoc - DatFirstStops.IndexTripStartInCleanData)
DatFirstStops.loc[:,"CorDir"] = DatFirstStops.groupby(['filename','IndexTripStartInCleanData'])['GetDir'].transform(lambda x: x==x.min())
DatFirstStops_AppxDir = DatFirstStops.query('CorDir')
SumDatWithGTFS.reset_index(inplace=True)
DatFirstStops_AppxDir = DatFirstStops_AppxDir.merge(SumDatWithGTFS,on =['filename',"IndexTripStartInCleanData"])
DatFirstStops_AppxDir.columns
DatFirstStops_AppxDir.drop(columns = ['fullpath','file_id','taglist'],inplace=True)
Check = DatFirstStops_AppxDir[['route_id','direction_id','route','pattern','dist',
                               'Dist_from_GTFS1stStop_start_ft','trip_headsign','stop_name','TripDurationFromTags',
                               'CrowFlyDistLatLongMi']]
DatFirstStops_AppxDir = DatFirstStops_AppxDir[DatFirstStops_AppxDir.CrowFlyDistLatLongMi>1]
Check = Check[Check.CrowFlyDistLatLongMi>1]
Check.groupby(['route_id','direction_id','pattern']).count()

temp = DatFirstStops_AppxDir[['filename',"IndexTripStartInCleanData","direction_id"]]
NearestRawnavOnGTFS_appxDir =temp.merge( NearestRawnavOnGTFS, on =['filename',"IndexTripStartInCleanData","direction_id"],
                          how = 'left')
NearestRawnavOnGTFS_appxDir = NearestRawnavOnGTFS_appxDir.query("route_id=='79'")
NearestRawnavOnGTFS_appxDir = NearestRawnavOnGTFS_appxDir.merge(SumDatWithGTFS,on =['filename',"IndexTripStartInCleanData"])
NearestRawnavOnGTFS_appxDir.drop(columns = ['fullpath','file_id','taglist'],inplace=True)

#10 Plot Rawnav Trace and Nearest Stops
########################################################################################
GroupsTemp =  NearestRawnavOnGTFS_appxDir.groupby(['filename','IndexTripStartInCleanData','route_id'])
FinDat = FinDat.query("route=='79'")
RawnavGrps = FinDat.groupby(['filename','IndexTripStartInCleanData','route'])
Stop = False
for name, RawNavGrp in RawnavGrps:
    Pattern = RawNavGrp["pattern"].values[0]
    Stop = False
    StopDat1 = GroupsTemp.get_group(name)
    wday = StopDat1["wday"].values[0]
    SaveFile= f"{wday}_{name[2]}_{Pattern}_{name[0]}_Row{int(name[1])}.html"
    wr.PlotRawnavTrajWithGTFS(RawNavGrp, StopDat1,path_processed_data,SaveFile)
    if Stop: break

















