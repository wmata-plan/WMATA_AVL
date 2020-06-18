# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Purpose: Merge GTFS and Rawnav data
Created on Fri May 15 15:36:49 2020
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
print(f"Run Section 1 Import Libraries and Set Global Parameters...")
begin_time = datetime.now() ##
import pandas as pd, os, sys, collections
#from geopy.distance import geodesic
from shapely.geometry import Point
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
    #TODO: Might need to edit following:
    path_processed_route_data = os.path.join(path_processed_data,"RouteData")
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
    path_processed_route_data = os.path.join(path_processed_data,"RouteData")

else:
    raise FileNotFoundError("Define the path_working, path_source_data, GTFS_Dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None 
restrict_n = None
MasterRouteList = ['S1','S2','S4','S9','70','79','64','G8','D32','H1','H2','H3','H4','H8','W47'] # 16 Gb RAM can't handle all these at one go
# AnalysisRoutes = ['S1','S9','H4','G8','64']
AnalysisRoutes = ['70','64','D32','H8','S2']
#AnalysisRoutes = ['S2','S4','H1','H2','H3','79','W47']]

ZipParentFolderName = "October 2019 Rawnav"

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")
print("*"*100)
# 2 Analyze Route ---Subset RawNav Data. 
###########################################################################################################################################################
print(f"Run Section 2 Analyze Route ---Subset RawNav Data...")
begin_time = datetime.now() ##
analysis_days = ['Monday']
DaysOfWeek = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
assert(len(set(DaysOfWeek)-set(analysis_days))> 0), print("""
                                                    analysis_days is a subset of following days: 
                                                    ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
                                                """)
# 2.1 Rawnav Data
############################################
FinDat = wr.readProcessedRawnav(
    AnalysisRoutes_=AnalysisRoutes,
    path_processed_route_data=path_processed_route_data,
    restrict=restrict_n,
    analysis_days=analysis_days)
# 2.2 Summary Data
############################################
FinSummaryDat, issueDat = wr.readSummaryRawnav(
    AnalysisRoutes_=AnalysisRoutes,
    path_processed_route_data=path_processed_route_data,
    restrict=restrict_n,
    analysis_days=analysis_days)
Subset = FinSummaryDat[['filename','IndexTripStartInCleanData']]
# 2.3 Merge Processed and Summary Data
############################################
FinDat = FinDat.merge(Subset,on=['filename','IndexTripStartInCleanData'],how='right')
set(FinDat.IndexTripStartInCleanData.unique()) -set(FinSummaryDat.IndexTripStartInCleanData.unique())
set(FinDat.filename.unique()) -set(FinSummaryDat.filename.unique())

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Analyze Route ---Subset RawNav Data : {executionTime}")
print("*"*100)
# 3 Read, analyze and summarize GTFS Data
###########################################################################################################################################################
print(f"Run Section 3 Read, analyze and summarize GTFS Data...")
begin_time = datetime.now() ##
# 3.1 Read the GTFS Data
############################################
GtfsData = wr.readGTFS(GTFS_Dir)
FirstStopDat1_rte, CheckFirstStop, CheckFirstStop1 = wr.get1ststop(GtfsData,AnalysisRoutes)
LastStopDat1_rte, CheckLastStop, CheckLastStop1 = wr.getlaststop(GtfsData,AnalysisRoutes)
wr.debugGTFS1stLastStopData(CheckFirstStop,CheckFirstStop1,CheckLastStop,CheckLastStop1,path_processed_data)

GtfsData.sort_values(['trip_id','stop_sequence'],inplace=True)
GtfsData.stop_id = GtfsData.stop_id.astype(str)
GtfsData_tripDef= GtfsData.groupby(['trip_id']).agg({'stop_id':lambda x:"_".join(x)})
GtfsData_tripDef = GtfsData_tripDef[~GtfsData_tripDef.duplicated('stop_id')].reset_index().rename(columns={'stop_id':"all_stops"})
GtfsData_tripDef = GtfsData_tripDef.merge(GtfsData,on="trip_id")
temp = GtfsData_tripDef.groupby('trip_id').stop_sequence.transform('idxmin').values
GtfsData_tripDef.loc[:,'1st_stopNm'] = GtfsData_tripDef.loc[temp,'stop_name'].values
 
# 3.2 Merge 1st stops to rawnav data
############################################
GtfsData_1stStops = GtfsData_tripDef.query('route_id in @MasterRouteList & stop_sequence==1')
NeedLastStop = GtfsData_1stStops.groupby(['route_id','direction_id','stop_name']).filter(lambda x: x['trip_id'].count()==2)
NearestRawnavOnGTFS_1stStop = wr.mergeStopsGTFSrawnav(GtfsData_1stStops, FinDat)

# 3.3 Merge last stops to rawnav data---Find correct direction---Handle route G8 and S9: Have the same start point but different end points.
############################################
# Just finding correct 1st stop from GTFS cannot tell us if a trip is long or short
# We also need to test the last stop to find if the trip is short or long. This would have happen if on 
# a route a trip starts from the same location but ends on two different stops.
SubsetRoutes = NeedLastStop.route_id.unique()
GtfsData_LastStops = GtfsData_tripDef.query('route_id in @SubsetRoutes')
temp = GtfsData_LastStops.groupby('trip_id').stop_sequence.transform(max)
GtfsData_LastStops = GtfsData_LastStops[GtfsData_LastStops.stop_sequence==temp]
FinDat2= FinDat.query('route in @SubsetRoutes')
if FinDat2.shape[0]!=0:
    NearestRawnavOnGTFS_LastStop = wr.mergeStopsGTFSrawnav(GtfsData_LastStops, FinDat2)
    # routeNoUnique1stStp = SubsetRoutes
    # SumDat_ = FinSummaryDat
    # Dat1stStop = NearestRawnavOnGTFS_1stStop
    # DatLastStop = NearestRawnavOnGTFS_LastStop 
    # 3.4 Find correct direction
    ############################################
    # Find Correct Dir
    # TODO Change name to Dat1stStopAndDir
    
Dat1stStopAndDir = wr.GetCorrectDirGTFS(NearestRawnavOnGTFS_1stStop,NearestRawnavOnGTFS_LastStop, FinSummaryDat, SubsetRoutes)
assert(sum(Dat1stStopAndDir.groupby(['filename', 'IndexTripStartInCleanData']).all_stops.count()!=1)==0) ,"might fail when long and short route start from the same 1st stop"
FinDat = FinDat.merge(Dat1stStopAndDir,on =['filename', 'IndexTripStartInCleanData'],how='right')

# 3.5 Merge all stops to rawnav data
############################################
# Get all stops on a bus route irrespective of short and long or direction. Will figure out the direction later.
# GtfsData_UniqueStops = GtfsData_tripDef[~GtfsData_tripDef.duplicated(['route_id','direction_id','stop_sequence','stop_name'],keep='first')]
# GtfsData_UniqueStops.sort_values(by=['route_id','direction_id','stop_sequence','stop_name'],inplace=True)
useAllStopId = True
NearestRawnavOnGTFS = wr.mergeStopsGTFSrawnav(GtfsData_tripDef, FinDat,useAllStopId)
NearestRawnavOnGTFS = NearestRawnavOnGTFS[['filename','IndexTripStartInCleanData','direction_id'
                                           ,'stop_sequence','IndexLoc','route_id','all_stops',
                                           'trip_headsign','stop_lat','stop_lon','stop_name',
                                           'Lat','Long','distNearestPointFromStop','geometry']]
NearestRawnavOnGTFS.sort_values(['filename','IndexTripStartInCleanData',
                                 'direction_id','stop_sequence'],inplace=True)
#Get data with correct stops
DatRawnavGTFS_CorDir, DatFirstLastStops, DatRawnavGTFS_issue = wr.FindStopOnRoute(NearestRawnavOnGTFS,FinSummaryDat)
SumDatWithGTFS= wr.GetSummaryGTFSdata(FinDat,FinSummaryDat,DatFirstLastStops)
SumDatWithGTFS.set_index(['fullpath','filename','file_id','wday','StartDateTime','EndDateTime','IndexTripStartInCleanData','taglist','route_pattern','route','pattern'],inplace=True,drop=True) 
# 3.6 Output Summary Files
############################################
OutFiSum = os.path.join(path_processed_data,f'GTFSTripSummaries.xlsx')
SumDatWithGTFS.to_excel(OutFiSum,merge_cells=False)

# 3.7 Output GTFS+Rawnav Merged Files
############################################
# TODO: output files

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 3 Read, analyze and summarize GTFS Data : {executionTime}")
print("*"*100)
# 4 Plot Rawnav Trace and Nearest Stops
###########################################################################################################################################################
print(f"Run Section 4 Plot Rawnav Trace and Nearest Stops...")
begin_time = datetime.now() ##
# 4.1 Add Summary data to Stop data from Plotting
############################################
DatRawnavGTFS_CorDir = DatRawnavGTFS_CorDir[['filename','IndexTripStartInCleanData','direction_id','stop_sequence','IndexLoc','stop_lat',
                                             'stop_lon','distNearestPointFromStop','geometry','stop_name']]
SumDatWithGTFS.reset_index(inplace=True)
NearestRawnavOnGTFS_appxDir = DatRawnavGTFS_CorDir.merge(SumDatWithGTFS, on = ['filename','IndexTripStartInCleanData'],how='left')
NearestRawnavOnGTFS_appxDir =\
NearestRawnavOnGTFS_appxDir[[ 'filename','wday', 'StartDateTime', 'EndDateTime','route_pattern','route', 'pattern','direction_id','stop_sequence','stop_name','stop_lat','stop_lon',\
 'IndexTripStartInCleanData','IndexTripEndInCleanData', 'distNearestPointFromStop','IndexLoc',\
 'StartOdomtFtGTFS','EndOdomtFtGTFS', 'TripDistMiGTFS','DistOdomMi','CrowFlyDistLatLongMi', 'StartSecPastStGTFS','EndSecPastStGTFS', 
 'TripDurSecGTFS','TripDurFromSec','TripDurationFromTags', 'StartLatGTFS', 'EndLatGTFS','StartLongGTFS', 'EndLongGTFS', 'StartDistFromGTFS1stStopFt'
 , 'TripSpeedMphGTFS','SpeedOdomMPH','SpeedTripTagMPH','SecStart', 'OdomFtStart', 'SecEnd',
       'OdomFtEnd', 'LatStart','LongStart', 'LatEnd', 'LongEnd','geometry']]
    
NearestRawnavOnGTFS_appxDir.rename(columns = {'IndexLoc':'ClosestIndexLocInRawnavTraj'},inplace=True)
# TODO : identify why the following na values are being added
# issueDat = NearestRawnavOnGTFS_appxDir[NearestRawnavOnGTFS_appxDir.route_pattern.isna()]
# issueDat = issueDat[['filename','IndexTripStartInCleanData']].merge(FinSummaryDat,on = ['filename','IndexTripStartInCleanData'])
# issueDat[~issueDat.duplicated(['filename','IndexTripStartInCleanData'])]
# 4.2 Plot Trajectories
############################################
GroupsTemp =  NearestRawnavOnGTFS_appxDir.groupby(['filename','IndexTripStartInCleanData','route'])
FinDat = FinDat.query("route in @AnalysisRoutes")
RawnavGrps = FinDat.groupby(['filename','IndexTripStartInCleanData','route'])

trackerUsableRte = collections.Counter()
for name, grp in GroupsTemp:
    Pattern = grp["pattern"].values[0]
    trackerUsableRte[f"{name[2]}_{Pattern}"]+= 1 
print(trackerUsableRte)
STOP = 5
tracker = collections.Counter()

len(RawnavGrps.groups.keys())
NearestRawnavOnGTFS_appxDir.route_pattern.unique()

for name, RawNavGrp in RawnavGrps:
    Pattern = RawNavGrp["pattern"].values[0]
    if name in GroupsTemp.groups:
        StopDat1 = GroupsTemp.get_group(name)   
    else: continue
    # if int(Pattern)!= 4: 
    #     continue
    tracker[f"{name[2]}_{Pattern}"]+=1 
    if tracker[f"{name[2]}_{Pattern}"]>=STOP: continue
    print(name, Pattern)
    wday = StopDat1["wday"].values[0]
    Hour = StopDat1.StartDateTime.values[0].split(" ")[1].split(":")[0]
    SaveFile= f"{wday}_{Hour}_{name[2]}_{Pattern}_{name[0]}_Row{int(name[1])}.html"
    map1 = wr.PlotRawnavTrajWithGTFS(RawNavGrp, StopDat1)
    SaveDir= os.path.join(path_processed_data,"TrajectoryFigures")
    if not os.path.exists(SaveDir):os.makedirs(SaveDir)
    SaveDir2= os.path.join(SaveDir,f'{name[2]}_{Pattern}')
    if not os.path.exists(SaveDir2):os.makedirs(SaveDir2)
    SaveDir3= os.path.join(path_processed_data,"TrajectoryFigures",f'{name[2]}_{Pattern}',wday)
    if not os.path.exists(SaveDir3):os.makedirs(SaveDir3)
    map1.save(os.path.join(SaveDir3,f"{SaveFile}"))

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 4 Plot Rawnav Trace and Nearest Stops : {executionTime}")
print("*"*100)
###########################################################################################################################################################
###########################################################################################################################################################
TestPlots = DatRawnavGTFS_issue.merge(FinSummaryDat,on = ['filename','IndexTripStartInCleanData'],how='left')
GroupsTempIssue =  TestPlots.groupby(['filename','IndexTripStartInCleanData','route'])
FinDat = FinDat.query("route in @AnalysisRoutes")
RawnavGrps = FinDat.groupby(['filename','IndexTripStartInCleanData','route'])

trackerRteIssues = collections.Counter()
for name, grp in GroupsTempIssue:
    Pattern = grp["pattern"].values[0]
    trackerRteIssues[f"{name[2]}_{Pattern}"]+= 1 
print(trackerRteIssues)
tracker = collections.Counter()
for name, RawNavGrp in RawnavGrps:
    Pattern = RawNavGrp["pattern"].values[0]
    if name in GroupsTempIssue.groups:
        StopDat1 = GroupsTempIssue.get_group(name)   
    else: continue
    tracker[f"{name[2]}_{Pattern}"]+= 1 
    tracker[f"{name[2]}_{Pattern}"]+=1 
    if tracker[f"{name[2]}_{Pattern}"]>=STOP: continue
    print(name, Pattern)
    wday = StopDat1["wday"].values[0]
    Hour = StopDat1.StartDateTime.values[0].split(" ")[1].split(":")[0]
    SaveFile= f"{wday}_{Hour}_{name[2]}_{Pattern}_{name[0]}_Row{int(name[1])}.html"
    map1 = wr.PlotRawnavTrajWithGTFS(RawNavGrp, StopDat1)
    SaveDir= os.path.join(path_processed_data,"DebugPlots")
    if not os.path.exists(SaveDir):os.makedirs(SaveDir)
    map1.save(os.path.join(SaveDir,f"{SaveFile}"))
