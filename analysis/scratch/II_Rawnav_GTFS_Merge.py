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
begin_time = datetime.now() ##
import pandas as pd, os, sys
#from geopy.distance import geodesic
from shapely.geometry import Point
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
    path_processed_data = os.path.join(path_source_data,"ProcessedData\BackupData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, GTFS_Dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None 
restrict_n = None
AnalysisRoutes = ['79']
ZipParentFolderName = "October 2019 Rawnav"

# 1.3 Import User-Defined Package
############################################
import wmatarawnav as wr

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 1 Import Libraries and Set Global Parameters : {executionTime}")

# 2 Analyze Route ---Subset RawNav Data. 
###########################################################################################################################################################
begin_time = datetime.now() ##
# 2.1 Rawnav Data
############################################
FinDat = pq.read_table(source =os.path.join(path_processed_data,"Route79_Partition.parquet"),\
filters =[('wday','=',"Monday"),('route','=',"79")]).to_pandas()
FinDat.route = FinDat.route.astype('str')
FinDat.drop(columns=['Blank','LatRaw','LongRaw','SatCnt','__index_level_0__'],inplace=True)
#Check for duplicate IndexLoc
assert(FinDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)

# 2.2 Summary Data
############################################
FinSummaryDat = pd.read_csv(os.path.join(path_processed_data,'TripSummaries.csv'))
FinSummaryDat.IndexTripStartInCleanData = FinSummaryDat.IndexTripStartInCleanData.astype('int32')
Issues = FinSummaryDat.query('TripDurFromSec < 600 | DistOdomMi < 2') # Trip should be atleast 5 min and 2 mile long
FinSummaryDat =FinSummaryDat .query('not (TripDurFromSec < 600 | DistOdomMi < 2)')
SumDat, SumDatStart, SumDatEnd = wr.subset_summary_data(FinSummaryDat, AnalysisRoutes)
Subset = SumDat[['filename','IndexTripStartInCleanData']]
FinDat = FinDat.merge(Subset,on=['filename','IndexTripStartInCleanData'],how='right')
set(FinDat.IndexTripStartInCleanData.unique()) -set(FinSummaryDat.IndexTripStartInCleanData.unique())

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 2 Analyze Route ---Subset RawNav Data : {executionTime}")

# 3 Read, analyze and summarize GTFS Data
###########################################################################################################################################################
begin_time = datetime.now() ##
# 3.1 Read the GTFS Data
############################################
GtfsData = wr.readGTFS(GTFS_Dir)
FirstStopDat1_rte, CheckFirstStop, CheckFirstStop1 = wr.get1ststop(GtfsData,AnalysisRoutes)
LastStopDat1_rte, CheckLastStop, CheckLastStop1 = wr.getlaststop(GtfsData,AnalysisRoutes)
wr.debugGTFS1stLastStopData(CheckFirstStop,CheckFirstStop1,CheckLastStop,CheckLastStop1,path_processed_data)

# 3.2 Merge all stops to rawnav data
############################################
# Get all stops on a bus route irrespective of short and long or direction. Will figure out the direction later.
GtfsData_UniqueStops = GtfsData[~GtfsData.duplicated(['route_id','direction_id','stop_name'],keep='first')]
NearestRawnavOnGTFS = wr.mergeStopsGTFSrawnav(GtfsData_UniqueStops, FinDat)
NearestRawnavOnGTFS = NearestRawnavOnGTFS[['filename','IndexTripStartInCleanData','direction_id'
                                           ,'stop_sequence','IndexLoc','route_id',
                                           'trip_headsign','stop_lat','stop_lon','stop_name'
                                           ,'Lat','Long','distNearestPointFromStop','geometry']]
NearestRawnavOnGTFS.sort_values(['filename','IndexTripStartInCleanData',
                                 'direction_id','stop_sequence'],inplace=True)
#Get data with correct direction
DatRawnavGTFS_CorDir, DatFirstLastStops = wr.GetCorrectDirGTFS(NearestRawnavOnGTFS,SumDat)
SumDatWithGTFS = wr.GetSummaryGTFSdata(FinDat,SumDat,DatFirstLastStops)
SumDatWithGTFS.set_index(['fullpath','filename','file_id','wday','StartDateTime','EndDateTime','IndexTripStartInCleanData','taglist','route_pattern','route','pattern'],inplace=True,drop=True) 
# 3.3 Output Summary Files
############################################
OutFiSum = os.path.join(path_processed_data,f'GTFSTripSummaries.xlsx')
SumDatWithGTFS.to_excel(OutFiSum,merge_cells=False)

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 3 Read, analyze and summarize GTFS Data : {executionTime}")

# 4 Plot Rawnav Trace and Nearest Stops
###########################################################################################################################################################
begin_time = datetime.now() ##
# 4.1 Add Summary data to Stop data from Plotting
############################################
DatRawnavGTFS_CorDir = DatRawnavGTFS_CorDir[['filename','IndexTripStartInCleanData','direction_id','stop_sequence','IndexLoc','stop_lat',
                                             'stop_lon','distNearestPointFromStop','geometry','stop_name']]
SumDatWithGTFS.reset_index(inplace=True)
NearestRawnavOnGTFS_appxDir = SumDatWithGTFS.merge(DatRawnavGTFS_CorDir, on = ['filename','IndexTripStartInCleanData'],how='right')
NearestRawnavOnGTFS_appxDir =\
NearestRawnavOnGTFS_appxDir[[ 'filename','wday', 'StartDateTime', 'EndDateTime','route', 'pattern','direction_id','stop_sequence','stop_name','stop_lat','stop_lon',\
 'IndexTripStartInCleanData','IndexTripEndInCleanData', 'distNearestPointFromStop','IndexLoc',\
 'StartOdomtFtGTFS','EndOdomtFtGTFS', 'TripDistMiGTFS','DistOdomMi','CrowFlyDistLatLongMi', 'StartSecPastStGTFS','EndSecPastStGTFS', 
 'TripDurSecGTFS','TripDurFromSec','TripDurationFromTags', 'StartLatGTFS', 'EndLatGTFS','StartLongGTFS', 'EndLongGTFS', 'StartDistFromGTFS1stStopFt'
 , 'TripSpeedMphGTFS','SpeedOdomMPH','SpeedTripTagMPH','SecStart', 'OdomFtStart', 'SecEnd',
       'OdomFtEnd', 'LatStart','LongStart', 'LatEnd', 'LongEnd','geometry']]
NearestRawnavOnGTFS_appxDir.rename(columns = {'IndexLoc':'ClosestIndexLocInRawnavTraj'},inplace=True)

# 4.2 Plot Trajectories
############################################
GroupsTemp =  NearestRawnavOnGTFS_appxDir.groupby(['filename','IndexTripStartInCleanData','route'])
FinDat = FinDat.query("route=='79'")
RawnavGrps = FinDat.groupby(['filename','IndexTripStartInCleanData','route'])

len(RawnavGrps.groups.keys())
Stop = 10
i = 0
for name, RawNavGrp in RawnavGrps:
    i=i+1
    print(name)
    Pattern = RawNavGrp["pattern"].values[0]
    if name in GroupsTemp.groups:
        StopDat1 = GroupsTemp.get_group(name)   
    else: continue
    wday = StopDat1["wday"].values[0]
    Hour = StopDat1.StartDateTime.values[0].split(" ")[1].split(":")[0]
    SaveFile= f"{wday}_{Hour}_{name[2]}_{Pattern}_{name[0]}_Row{int(name[1])}.html"
    map1 = wr.PlotRawnavTrajWithGTFS(RawNavGrp, StopDat1)
    SaveDir= os.path.join(path_processed_data,"TrajectoryFigures")
    if not os.path.exists(SaveDir):os.makedirs(SaveDir)
    SaveDir2= os.path.join(path_processed_data,"TrajectoryFigures",f'{name[2]}_{Pattern}')
    if not os.path.exists(SaveDir2):os.makedirs(SaveDir2)
    map1.save(os.path.join(SaveDir2,f"{SaveFile}"))
    if i==Stop: break

executionTime= str(datetime.now() - begin_time).split('.')[0]
print(f"Run Time Section 4 Plot Rawnav Trace and Nearest Stops : {executionTime}")

###########################################################################################################################################################
###########################################################################################################################################################








