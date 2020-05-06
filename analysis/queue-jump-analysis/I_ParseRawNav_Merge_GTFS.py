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
from shapely.ops import nearest_points
from shapely.geometry import Point
from shapely.geometry import LineString

import geopandas as gpd
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore") #Too many Pandas warnings
DEBUG = False
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
    DEBUG = False
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
# restrict_n = 500
restrict_n = 500

AnalysisRoutes = ['79','X2','X9','U6','H4']
ZipParentFolderName = "October 2019 Rawnav"
# Assumes directory structure:
# ZipParentFolderName (e.g, October 2019 Rawnav)
#  -- ZippedFilesDirs (e.g., Vehicles 0-2999.zip)
#     -- FileUniverse (items in various ZippedFilesDirs ala rawnav##########.txt.zip
#

#2 Indentify Relevant Files for Analysis Routes
########################################################################################

#Extract parent zipped folder and get the zipped files path
ZippedFilesDirParent = os.path.join(path_source_data, ZipParentFolderName)
ZippedFilesDirs = glob.glob(os.path.join(path_source_data,ZipParentFolderName,'Vehicles *.zip'))
UnZippedFilesDir =  glob.glob(os.path.join(path_source_data,ZippedFilesDirParent,'Vehicles*[0-9]'))

FileUniverse = wr.GetZippedFilesFromZipDir(ZippedFilesDirs,ZippedFilesDirParent) 

# Return a dataframe of routes and details
rawnav_inventory = wr.find_rawnav_routes(FileUniverse, nmax = restrict_n, quiet = False)

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


CleanDataDict = {}
for key, datadict in RouteRawTagDict.items():
    CleanDataDict[key] = wr.clean_rawnav_data(datadict)

#DEBUG
if DEBUG:
    RouteRawTagDict.keys()
    key = 'rawnav02616191007.txt'
    rawnavdata =data = RouteRawTagDict[key]['RawData']
    taglineData = RouteRawTagDict[key]['tagLineInfo']

# TODO: Need to write processed data to database, HDF5, Feather, or parquet format.

FinSummaryDat = pd.DataFrame()
for keys,datadict in CleanDataDict.items():
    FinSummaryDat = pd.concat([FinSummaryDat, datadict['SummaryData']])

if DEBUG:
    FinSummaryDat1 = FinSummaryDat.copy()
    FinSummaryDat1.loc[:,"FileNm"] = FinSummaryDat1.file_id.astype('int64')
    InFile = os.path.join(path_processed_data,'TripSummaries_Veh0_2999 V1.xlsx')
    x1 = pd.ExcelFile(InFile); x1.sheet_names; CheckData = x1.parse('SummaryData')
    CheckData.dtypes; FinSummaryDat.dtypes
    CheckData.loc[:,"FileNm"] = CheckData.FileNm.astype('int64')
    CheckData.loc[:,'StartDateTime'] = pd.to_datetime(CheckData['Date']+" "+CheckData['TripStartTime'])

    CheckData1 = pd.merge(FinSummaryDat1, CheckData, on =["FileNm",'StartDateTime'],how="inner")
    CheckData1.columns
    CheckData2 =CheckData1[['file_id','route_pattern','tag_busid','route','Tag','BusID','StartDateTime',
                'EndDateTime','Date','TripStartTime','TripEndTime','CrowFlyDistLatLongMi',
                'Dist_from_LatLong','SpeedOdomMPH','TripSpeed_RawData','SpeedTripTagMPH','TripSpeed_Tags',
                'DistOdomMi','DistanceMi','TripDurFromSec','TripDurationFromRawData',
                'TripDurationFromTags_x','TripDurationFromTags_y']]
    assert((CheckData2.route_pattern.str.strip() ==CheckData2.Tag.str.strip()).all())
    CheckData3 = CheckData2[~(CheckData2.TripDurationFromTags_x.isna())]
    assert(((CheckData3.TripDurationFromTags_x.dt.total_seconds()-\
             CheckData3.TripDurationFromTags_y)<0.001).all())
    CheckData3 = CheckData2[~(CheckData2.SpeedOdomMPH.isna())]
    assert(((CheckData3.SpeedOdomMPH-CheckData3.TripSpeed_RawData)<2).all())
    CheckData3 = CheckData2[~(CheckData2.TripDurFromSec.isna())]
    assert((CheckData3.TripDurFromSec-CheckData3.TripDurationFromRawData<0.0000001).all())
    assert((CheckData2.DistanceMi-CheckData2.DistOdomMi<0.0000001).all())
    assert((CheckData2.CrowFlyDistLatLongMi-CheckData2.Dist_from_LatLong<0.1).all())


#Output Summary Files
now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H")

OutFiSum = os.path.join(path_processed_data,f'TripSummaries_{d4}.csv')
FinSummaryDat.to_csv(OutFiSum)


#5 Analyze Route 79---Subset RawNav Data. 
# TODO: Extend it to handle multiple routes 

# Didn't get a chance to clean-up the code before
# I might have to conduct some preliminary analysis-
# so likely would get to the GTFS function clean-up next 
# week (Week of May 11th)
########################################################################################
rte_id = "79"
FinDat79 = pd.DataFrame()
SearchDF = rawnav_inventory_filtered[['route','filename']].set_index('route')
Route79Files = (SearchDF.loc[rte_id,:].values).flatten()
for file in Route79Files:
    tempDf =CleanDataDict[file]['rawnavdata']
    tempDf.loc[:,"filename"] = file
    FinDat79 = pd.concat([FinDat79,tempDf])
FinDat79.reset_index(drop=True,inplace=True)
FinDat79 = FinDat79.query("route==@rte_id")
SumData79 = FinSummaryDat.query("route==@rte_id")
SumData79.reset_index(drop=True,inplace=True)
SumData79.columns
tempDf = SumData79[['filename','IndexTripStartInCleanData','LatStart', 'LongStart']]
geometryStart = [Point(xy) for xy in zip(tempDf.LongStart, tempDf.LatStart)]
SumData79_StartGpd=gpd.GeoDataFrame(tempDf, geometry=geometryStart,crs={'init':'epsg:4326'})

tempDf = SumData79[['filename','IndexTripStartInCleanData','LatEnd', 'LongEnd']]
geometryEnd = [Point(xy) for xy in zip(tempDf.LongEnd, tempDf.LatEnd)]
SumData79_EndGpd=gpd.GeoDataFrame(tempDf, geometry=geometryEnd,crs={'init':'epsg:4326'})
#6 Read the GTFS Data
########################################################################################
StopsDat= pd.read_csv(os.path.join(GTFS_Dir,"stops.txt"))
StopTimeDat = pd.read_csv(os.path.join(GTFS_Dir,"stop_times.txt"))
TripsDat =pd.read_csv(os.path.join(GTFS_Dir,"trips.txt"))
StopsDat= StopsDat[['stop_id','stop_name','stop_lat','stop_lon']]
StopTimeDat = StopTimeDat[['trip_id','arrival_time','departure_time','stop_id','stop_sequence','pickup_type','drop_off_type']]
TripsDat = TripsDat[['route_id','service_id','trip_id','trip_headsign','direction_id']]
# with pd.option_context('display.max_rows', 5, 'display.max_columns', 10):
#     display(StopTimeDat)

#3 Analyze the Trip Start and End
########################################################################################
#trip_id is a unique identifier irrespective of the route
TripsDat.trip_id = TripsDat.trip_id.astype(str)
StopTimeDat.trip_id = StopTimeDat.trip_id.astype(str)
# 0: travel in one direction; 1 travel in opposite direction
TripSumDa = TripsDat.groupby(['route_id','direction_id','trip_headsign']).count().reset_index()
Merdat = TripsDat.merge(StopTimeDat,on="trip_id",how='inner')
Merdat = Merdat.merge(StopsDat,on= "stop_id")

#Get the 1st stops
#########################################
Mask_1stStop = (Merdat.stop_sequence ==1)
FirstStopDat = Merdat[Mask_1stStop]
FirstStopDat.rename(columns = {'stop_id':'first_stopId','stop_name':"first_stopNm",'stop_lat':'first_sLat',
                               'stop_lon':'first_sLon'
                               },inplace=True)
FirstStopDat = FirstStopDat[['route_id','direction_id','trip_headsign',
                             'first_stopId',"first_stopNm",'first_sLat',
                             'first_sLon','arrival_time','departure_time']]
def to_set(x):
    return set(x)
CheckFirstStop = FirstStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'first_stopId':to_set,'first_stopNm':to_set})
CheckFirstStop1 = FirstStopDat.groupby(['route_id','direction_id','trip_headsign',\
                                        'first_stopId','first_stopNm'])\
                                        .agg({'first_sLat':['count','first'],'first_sLon':'first'
                                             ,'arrival_time':to_set,'departure_time':to_set})

#Get Last stops per trip
#########################################
TempDa = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index()
LastStopDat = TempDa.merge(Merdat, on=['trip_id','stop_sequence'],how='left')
LastStopDat.rename(columns = {'stop_id':'last_stopId','stop_name':"last_stopNm",'stop_lat':'last_sLat',
                               'stop_lon':'last_sLon'},inplace=True)
LastStopDat = LastStopDat[['route_id','direction_id','trip_headsign',
                           'last_stopId',"last_stopNm",'last_sLat','last_sLon','arrival_time','departure_time']]
CheckLastStop = LastStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'last_stopId':to_set,'last_stopNm':to_set})

CheckLastStop1 = LastStopDat.groupby(['route_id','direction_id','trip_headsign',
                                      'last_stopId','last_stopNm'])\
                                    .agg({'last_sLat':['count','first'],
                                          'last_sLon':'first' ,'arrival_time':to_set,'departure_time':to_set})
First_Last_Stop = CheckFirstStop.merge(CheckLastStop,left_index=True,right_index=True,how='left')
#Check stops Data
#########################################
OutFi = os.path.join(path_processed_data,'First_Last_Stop.xlsx')
writer = pd.ExcelWriter(OutFi)
First_Last_Stop.to_excel(writer,'First_Last_Stop')
CheckFirstStop1.to_excel(writer,'First_Stop')
CheckLastStop1.to_excel(writer,'Last_Stop')
writer.save()


#4 Get first stops and lasts stops by route---ignore direction
########################################################################################
dropCols = ['direction_id','trip_headsign','arrival_time','departure_time']
FirstStopDat1 =FirstStopDat.drop(columns=dropCols)        
FirstStopDat1 = FirstStopDat1.groupby(['route_id','first_stopId','first_stopNm']).first()
LastStopDat1 =LastStopDat.drop(columns=dropCols)        
LastStopDat1 = LastStopDat1.groupby(['route_id','last_stopId','last_stopNm']).first()
FirstStopDat1.to_csv(os.path.join(path_processed_data,'FirstStopGTFS.csv'))
LastStopDat1.to_csv(os.path.join(path_processed_data,'LastStopGTFS.csv'))

#TODO: Create a function for this: 
FirstStopDat1.reset_index(inplace=True);LastStopDat1.reset_index(inplace=True)
FirstStopDat1_rte = FirstStopDat1.query('route_id==@rte_id') 
LastStopDat1_rte = LastStopDat1.query('route_id==@rte_id') 

geometryStart = [Point(xy) for xy in zip(FirstStopDat1_rte.first_sLon, FirstStopDat1_rte.first_sLat)]
FirstStopDat1_rte=gpd.GeoDataFrame(FirstStopDat1_rte, geometry=geometryStart,crs={'init':'epsg:4326'})
#TODO: Need a function
geometryEnd = [Point(xy) for xy in zip(LastStopDat1_rte.last_sLon, LastStopDat1_rte.last_sLat)]
LastStopDat1_rte=gpd.GeoDataFrame(LastStopDat1_rte, geometry=geometryEnd,crs={'init':'epsg:4326'})

#https://stackoverflow.com/questions/56520780/how-to-use-geopanda-or-shapely-to-find-nearest-point-in-same-geodataframe
SumData79_StartGpd.insert(3, 'nearest_start', None)
SumData79_EndGpd.insert(3, 'nearest_end', None)

for index, row in SumData79_StartGpd.iterrows():
    point = row.geometry
    multipoint = FirstStopDat1_rte.geometry.unary_union
    queried_geom, nearest_geom = nearest_points(point, multipoint)
    SumData79_StartGpd.loc[index, 'nearest_start'] = nearest_geom
    
for index, row in SumData79_EndGpd.iterrows():
    point = row.geometry
    multipoint = LastStopDat1_rte.geometry.unary_union
    queried_geom, nearest_geom = nearest_points(point, multipoint)
    SumData79_EndGpd.loc[index, 'nearest_end'] = nearest_geom
    
SumData79_StartGpd = SumData79_StartGpd[['filename','IndexTripStartInCleanData','nearest_start']]
SumData79_EndGpd = SumData79_EndGpd[['filename','IndexTripStartInCleanData','nearest_end']]


geometry = [Point(xy) for xy in zip(FinDat79.Long.astype(float), FinDat79.Lat.astype(float))]
FinDat79gpd=gpd.GeoDataFrame(FinDat79, geometry=geometry,crs={'init':'epsg:4326'})
FinDat79gpd = FinDat79gpd.merge(SumData79_StartGpd,on =['filename','IndexTripStartInCleanData'],how='left')
FinDat79gpd = FinDat79gpd.merge(SumData79_EndGpd,on =['filename','IndexTripStartInCleanData'],how='left')

geometry1= [Point(xy) for xy in zip(FinDat79.Long.astype(float), FinDat79.Lat.astype(float))]
geometry2 =FinDat79gpd.nearest_start
geometry = [LineString(list(xy)) for xy in zip(geometry1,geometry2)]
geometry1 = geometry2 = None
FinDat79gpd.geometry = geometry
FinDat79gpd.to_crs(epsg=3310,inplace=True) #meters
FinDat79gpd.loc[:,'distances_start_ft'] = FinDat79gpd.geometry.length *3.28084

geometry3= [Point(xy) for xy in zip(FinDat79.Long.astype(float), FinDat79.Lat.astype(float))]
geometry4 =FinDat79gpd.nearest_end
geometry5 = [LineString(list(xy)) for xy in zip(geometry3,geometry4)]
geometry3 = geometry4 = None
tempCols = list(FinDat79gpd.columns); tempCols.remove('geometry')
FinDat79gpd = FinDat79gpd[tempCols]
FinDat79gpd=gpd.GeoDataFrame(FinDat79gpd, geometry=geometry5,crs={'init':'epsg:4326'})
FinDat79gpd.to_crs(epsg=3310,inplace=True) #meters
FinDat79gpd.loc[:,'distances_end_ft'] = FinDat79gpd.geometry.length *3.28084

checkDat =FinDat79gpd.iloc[0:200,:]
# breakpoint()
#AxB: Something is wrong in above distances. Will check tomorrow.
#****************************************************************************************************************




# TODO: Delete most of the stuff below:
MinDat = FinDat79gpd.groupby(['filename','IndexTripStartInCleanData'])['distances_start_ft','distances_end_ft'].idxmin().reset_index()
MinDat.rename(columns = {'distances_start_ft':'LowerBoundLoc','distances_end_ft':"UpperBoundLoc"},inplace=True)
MinDat.loc[:,'LowerBound'] = FinDat79gpd.loc[MinDat.loc[:,'LowerBoundLoc'],'IndexLoc'].values
MinDat.loc[:,'UpperBound'] = FinDat79gpd.loc[MinDat.loc[:,'UpperBoundLoc'],'IndexLoc'].values
MinDat.drop(columns=['LowerBoundLoc','UpperBoundLoc'],inplace=True)
MinDat.reset_index(inplace=True)
CheckDat = None
FinDat79gpd = FinDat79gpd.merge(MinDat,on=['filename','IndexTripStartInCleanData'],how='left')
CheckDat = FinDat79gpd.iloc[0:5,:]
FinDat79gpd1 = FinDat79gpd.query('IndexLoc>=LowerBound & IndexLoc<=UpperBound')
FinDat79gpd1.rename(columns= {'distances_start_ft':'Dist_from_GTFS1stStop',
                              'distances_end_ft':'Dist_from_GTFSlastStop'},inplace=True)
FinDat79gpd1 = FinDat79gpd1[['filename','IndexTripStartInCleanData','Lat','Long','Heading','OdomtFt','SecPastSt'
                       ,'Dist_from_GTFS1stStop','Dist_from_GTFSlastStop']]

Map1 = lambda x: max(x)-min(x)
SumDat1 =FinDat79gpd1.groupby(['filename','IndexTripStartInCleanData']).agg({'OdomtFt':['min','max',Map1],
                                                 'SecPastSt':['min','max',Map1],
                                                 'Lat':['first','last'],
                                                 'Long':['first','last'],
                                                 'Dist_from_GTFS1stStop':['first','last'],
                                                 'Dist_from_GTFSlastStop':['last']})
SumDat1.columns = ['OdomtFt_start_GTFS','OdomtFt_end_GTFS','Trip_Dist_Mi_GTFS',
                   'SecPastSt_start_GTFS','SecPastSt_end_GTFS','Trip_Dur_Sec_GTFS',
                   'Lat_start_GTFS','Lat_end_GTFS','Long_start_GTFS','Long_end_GTFS',
                   'Dist_from_GTFS1stStop_start_ft','Dist_from_GTFS1stStop_end_mi',
                   'Dist_from_GTFSlastStop_end_ft']
SumDat1.loc[:,['Trip_Dist_Mi_GTFS','Dist_from_GTFS1stStop_end_mi']] =SumDat1.loc[:,['Trip_Dist_Mi_GTFS','Dist_from_GTFS1stStop_end_mi']]/5280
SumDat1.loc[:,'Trip_Speed_mph_GTFS'] =round(3600* SumDat1.Trip_Dist_Mi_GTFS/SumDat1.Trip_Dur_Sec_GTFS,2)
SumDat1 = SumDat1.merge(SumData79,on=['filename','IndexTripStartInCleanData'],how='left')
#Output Summary Files
now = datetime.now()
d4 = now.strftime("%b-%d-%Y %H")

OutFiSum = os.path.join(path_processed_data,f'TripSummaries_{d4}.xlsx')
SumDat1.to_excel(OutFiSum)



