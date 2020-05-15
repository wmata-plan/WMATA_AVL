# -*- coding: utf-8 -*-
"""
Created on Fri May 15 15:36:49 2020

@author: abibeka
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

#5 Analyze Route ---Subset RawNav Data. 
########################################################################################
FinDat = pq.read_table(source =os.path.join(path_processed_data,"Route79_Partition.parquet")).to_pandas()
FinDat = pq.read_table(source =os.path.join(path_processed_data,"Route79_Partition.parquet")).to_pandas()
FinDat.drop(columns="__index_level_0__",inplace=True)
FinDat.route = FinDat.route.astype('str')

set(FinDat.IndexTripStartInCleanData.unique()) -set(FinSummaryDat.IndexTripStartInCleanData.unique())
# 5.1 Summary Data
########################################################################################
FinSummaryDat = pd.read_csv(os.path.join(path_processed_data,'TripSummaries.csv'))
FinSummaryDat.IndexTripStartInCleanData = FinSummaryDat.IndexTripStartInCleanData.astype('int32')
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
OutFiSum = os.path.join(path_processed_data,f'GTFSTripSummaries.xlsx')
SumDatWithGTFS.to_excel(OutFiSum,merge_cells=False)

#8 Merge all stops to rawnav data
########################################################################################
GtfsData_UniqueStops = GtfsData[~GtfsData.duplicated(['route_id','direction_id','stop_name'],keep='first')]
NearestRawnavOnGTFS = wr.mergeStopsGTFSrawnav(GtfsData_UniqueStops, FinDat)
NearestRawnavOnGTFS = NearestRawnavOnGTFS[['filename','IndexTripStartInCleanData','direction_id'
                                           ,'stop_sequence','IndexLoc','route_id',
                                           'trip_headsign','stop_lat','stop_lon','stop_name'
                                           ,'Lat','Long','distNearestPointFromStop','geometry']]
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
Check = DatFirstStops_AppxDir[['route_id','direction_id','route','pattern','distNearestPointFromStop',
                               'StartDistFromGTFS1stStopFt','trip_headsign','stop_name','TripDurationFromTags',
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

NearestRawnavOnGTFS_appxDir =\
NearestRawnavOnGTFS_appxDir[[ 'filename','wday', 'StartDateTime', 'EndDateTime', 'route', 'pattern', 'route_pattern',\
 'IndexTripStartInCleanData','IndexTripEndInCleanData', 
 'direction_id','stop_sequence', 'IndexLoc', 'route_id', 'trip_headsign', \
 'stop_lat','stop_lon', 'stop_name', 'Lat', 'Long', 'distNearestPointFromStop', 
 'StartOdomtFtGTFS','EndOdomtFtGTFS', 'TripDistMiGTFS', 'StartSecPastStGTFS','EndSecPastStGTFS', 
 'TripDurSecGTFS', 'StartLatGTFS', 'EndLatGTFS','StartLongGTFS', 'EndLongGTFS', 'StartDistFromGTFS1stStopFt',
  'EndDistFromGTFSlastStopFt', 'TripSpeedMphGTFS','SecStart', 'OdomFtStart', 'SecEnd',
       'OdomFtEnd', 'TripDurFromSec', 'TripDurationFromTags', 'DistOdomMi',
       'SpeedOdomMPH', 'SpeedTripTagMPH', 'CrowFlyDistLatLongMi', 'LatStart',
       'LongStart', 'LatEnd', 'LongEnd','geometry']]
NearestRawnavOnGTFS_appxDir.rename(columns = {'IndexLoc':'ClosestIndexLocInRawnavTraj'},inplace=True)

#10 Plot Rawnav Trace and Nearest Stops
########################################################################################
GroupsTemp =  NearestRawnavOnGTFS_appxDir.groupby(['filename','IndexTripStartInCleanData','route_id'])
FinDat = FinDat.query("route=='79'")
RawnavGrps = FinDat.groupby(['filename','IndexTripStartInCleanData','route'])
Stop = False
for name, RawNavGrp in RawnavGrps:
    Pattern = RawNavGrp["pattern"].values[0]
    Stop = False
    if name in GroupsTemp.groups:
        StopDat1 = GroupsTemp.get_group(name)   
    else: continue
    wday = StopDat1["wday"].values[0]
    Hour = StopDat1.StartDateTime.values[0].split(" ")[1].split(":")[0]
    SaveFile= f"{wday}_{Hour}_{name[2]}_{Pattern}_{name[0]}_Row{int(name[1])}.html"
    wr.PlotRawnavTrajWithGTFS(RawNavGrp, StopDat1,path_processed_data,SaveFile)
    if Stop: break










