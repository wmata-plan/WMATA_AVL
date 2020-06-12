# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Created on Tue Apr 28 15:07:59 2020
Purpose: Functions for processing rawnav & GTFS data data
"""
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.ops import nearest_points
from scipy.spatial import cKDTree
import numpy as np
import folium
from folium import plugins
import pyarrow.parquet as pq


# readProcessedRawnav
###################################################################################################################################
def readProcessedRawnav(AnalysisRoutes_,path_processed_route_data, restrict, analysis_days):
    tempList= []
    FinDat= pd.DataFrame()
    for analysisRte in AnalysisRoutes_:
        filterParquet = [[('wday','=',day)] for day in analysis_days]
        tempDat = pq.read_table(source =os.path.join(path_processed_route_data,f"Route{analysisRte}_Restrict{restrict}.parquet")\
        ,filters =filterParquet).to_pandas()
        tempDat.route = tempDat.route.astype('str')
        tempDat.drop(columns=['Blank','LatRaw','LongRaw','SatCnt','__index_level_0__'],inplace=True)
        #Check for duplicate IndexLoc
        assert(tempDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)
        tempList.append(tempDat)
    FinDat = pd.concat(tempList)
    return(FinDat)

# readSummaryRawnav
###################################################################################################################################
def readSummaryRawnav(AnalysisRoutes_,path_processed_route_data, restrict,analysis_days):
    tempList= []
    issueList =[]
    FinIssueDat = pd.DataFrame()
    FinSummaryDat= pd.DataFrame()
    for analysisRte in AnalysisRoutes_:
        tempSumDat = pd.read_csv(os.path.join(path_processed_route_data,f'TripSummaries_Route{analysisRte}_Restrict{restrict}.csv'))
        tempSumDat.IndexTripStartInCleanData = tempSumDat.IndexTripStartInCleanData.astype('int32')
        tempSumDat  = tempSumDat.query('wday in @analysis_days')
        if tempSumDat.shape[0]==0: raise ValueError(f"No trips on any of the analysis_days ({analysis_days})")
        issueDat = tempSumDat.query('TripDurFromSec < 600 | DistOdomMi < 2') # Trip should be atleast 5 min and 2 mile long
        tempSumDat =tempSumDat .query('not (TripDurFromSec < 600 | DistOdomMi < 2)')
        print(f'Removing {issueDat.shape[0]} out of {tempSumDat.shape[0]} trips/ rows with TripDurFromSec < 600 seconds or DistOdomMi < 2 miles from route {analysisRte}')
        tempList.append(tempSumDat)
        issueList.append(issueDat)
    FinSummaryDat = pd.concat(tempList)
    FinIssueDat = pd.concat(issueList)
    return(FinSummaryDat,FinIssueDat)




# readGTFS
###################################################################################################################################
def readGTFS(GTFS_Dir_):
    '''
    Parameters
    ----------
    GTFS_Dir_ : str
        full path to GTFS directory.
    Returns
    -------
    Merdat : pd.DataFrame
        Merged stop, stop time, and trip data from GTFS.
    '''
    StopsDat= pd.read_csv(os.path.join(GTFS_Dir_,"stops.txt")) #stops data---has stop_id and stop lat-long
    StopTimeDat = pd.read_csv(os.path.join(GTFS_Dir_,"stop_times.txt")) #stoptime data---has trip_id and stop_sequence
    TripsDat =pd.read_csv(os.path.join(GTFS_Dir_,"trips.txt")) #trip data---has route_id and trip_id
    StopsDat= StopsDat[['stop_id','stop_name','stop_lat','stop_lon']]
    StopTimeDat = StopTimeDat[['trip_id','arrival_time','departure_time','stop_id','stop_sequence','pickup_type','drop_off_type']]
    TripsDat = TripsDat[['route_id','service_id','trip_id','trip_headsign','direction_id']]
    #trip_id is a unique identifier irrespective of the route
    TripsDat.trip_id = TripsDat.trip_id.astype(str)
    StopTimeDat.trip_id = StopTimeDat.trip_id.astype(str)
    # 0: travel in one direction; 1 travel in opposite direction
    Merdat = TripsDat.merge(StopTimeDat,on="trip_id",how='inner')
    Merdat = Merdat.merge(StopsDat,on= "stop_id")
    return(Merdat)
###################################################################################################################################

# get1ststop
###################################################################################################################################
def get1ststop(Merdat,AnalysisRoutes_):
    '''
    Parameters
    ----------
    Merdat : pd.DataFrame
        Merged stop, stop time, and trip data from GTFS..
    AnalysisRoutes_ : list
        list of routes that need to be analyzed.
    Returns
    -------
    FirstStopDat1_rte : pd.DataFrame
        Long data with information on unique 1st stops per route. 
    CheckFirstStop : pd.DataFrame
        Data with unique 1st stops by directions in a set---for debugging
    CheckFirstStop1 : pd.DataFrame
        Data with info on frequency of location, arrival, and departures of different 1st stop on a route---for debugging
    '''
    # Get the 1st stops
    ########################################################################################
    Mask_1stStop = (Merdat.stop_sequence ==1)
    FirstStopDat = Merdat[Mask_1stStop]
    FirstStopDat.rename(columns = {'stop_id':'first_stopId','stop_name':"first_stopNm",'stop_lat':'first_sLat',
                                   'stop_lon':'first_sLon'
                                   },inplace=True)
    FirstStopDat = FirstStopDat[['route_id','direction_id','trip_headsign',
                                 'first_stopId',"first_stopNm",'first_sLat',
                                 'first_sLon','arrival_time','departure_time']]
    # Get unique first stops by route---ignore direction
    ########################################################################################
    dropCols = ['direction_id','trip_headsign','arrival_time','departure_time']
    FirstStopDat1 =FirstStopDat.drop(columns=dropCols)        
    FirstStopDat1 = FirstStopDat1.groupby(['route_id','first_stopId','first_stopNm']).first() #For each route get all unique 1st stops
    FirstStopDat1.reset_index(inplace=True)
    FirstStopDat1_rte = FirstStopDat1.query('route_id in @AnalysisRoutes_') # subset data for analysis routes
    def to_set(x):
        return set(x)
    # Debugging data
    ########################################################################################
    CheckFirstStop = FirstStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'first_stopId':to_set,'first_stopNm':to_set})
    #get the unique 1st stops by directions in a set. 
    CheckFirstStop1 = FirstStopDat.groupby(['route_id','direction_id','trip_headsign',\
                                            'first_stopId','first_stopNm'])\
                                            .agg({'first_sLat':['count','first'],'first_sLon':'first'
                                                 ,'arrival_time':to_set,'departure_time':to_set})
    #get the info on frequency of location, arrival, and departures of different 1st stop on a route                                
    return(FirstStopDat1_rte, CheckFirstStop, CheckFirstStop1)
###################################################################################################################################

# getlaststop
#################################################################################################################
def getlaststop(Merdat,AnalysisRoutes_):
    '''
    Parameters
    ----------
    Merdat : pd.DataFrame
        Merged stop, stop time, and trip data from GTFS..
    AnalysisRoutes_ : list
        list of routes that need to be analyzed.
    Returns
    -------
    LastStopDat1_rte : pd.DataFrame
        Long data with information on unique last stops per route. 
    CheckLastStop : pd.DataFrame
        Data with unique last stops by directions in a set---for debugging
    CheckLastStop1 : pd.DataFrame
        Data with info on frequency of location, arrival, and departures of different last stop on a route---for debugging
    '''
    # Get Last stops per trip
    ########################################################################################
    TempDa = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index() 
    LastStopDat = TempDa.merge(Merdat, on=['trip_id','stop_sequence'],how='left')
    LastStopDat.rename(columns = {'stop_id':'last_stopId','stop_name':"last_stopNm",'stop_lat':'last_sLat',
                                   'stop_lon':'last_sLon'},inplace=True)
    LastStopDat = LastStopDat[['route_id','direction_id','trip_headsign',
                               'last_stopId',"last_stopNm",'last_sLat','last_sLon','arrival_time','departure_time']]
    # Get lasts stops by route---ignore direction
    ########################################################################################
    dropCols = ['direction_id','trip_headsign','arrival_time','departure_time']
    LastStopDat1 =LastStopDat.drop(columns=dropCols)        
    LastStopDat1 = LastStopDat1.groupby(['route_id','last_stopId','last_stopNm']).first() #For each route get all unique last stops
    LastStopDat1.reset_index(inplace=True)
    LastStopDat1_rte = LastStopDat1.query('route_id in @AnalysisRoutes_') # subset data for analysis routes
    def to_set(x):
        return set(x)
    #get the unique last stops by directions in a set. 
    CheckLastStop = LastStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'last_stopId':to_set,'last_stopNm':to_set})
    #get the info on frequency of location, arrival, and departures of different last stops on a route                                
    CheckLastStop1 = LastStopDat.groupby(['route_id','direction_id','trip_headsign',
                                          'last_stopId','last_stopNm'])\
                                        .agg({'last_sLat':['count','first'],
                                              'last_sLon':'first' ,'arrival_time':to_set,'departure_time':to_set})
    return(LastStopDat1_rte, CheckLastStop, CheckLastStop1)
###################################################################################################################################

# debugGTFS1stLastStopData
###################################################################################################################################                          
def debugGTFS1stLastStopData(CheckFirstStop,CheckFirstStop1,CheckLastStop,CheckLastStop1,path_processed_data_):
    '''
    Parameters
    ----------
    CheckFirstStop : pd.DataFrame
        Data with unique 1st stops by directions in a set---for debugging
    CheckFirstStop1 : pd.DataFrame
        Data with info on frequency of location, arrival, and departures of different 1st stop on a route---for debugging
    CheckLastStop : pd.DataFrame
        Data with unique last stops by directions in a set---for debugging
    CheckLastStop1 : pd.DataFrame
        Data with info on frequency of location, arrival, and departures of different last stop on a route---for debugging
    path_processed_data_ : str
        path for storing the output data.
    Returns
    -------
    None.
    '''
    First_Last_Stop = CheckFirstStop.merge(CheckLastStop,left_index=True,right_index=True,how='left')
    #Check stops Data
    #########################################
    OutFi = os.path.join(path_processed_data_,'First_Last_Stop.xlsx')
    writer = pd.ExcelWriter(OutFi) # excel writer object
    First_Last_Stop.to_excel(writer,'First_Last_Stop') # sheet 1
    CheckFirstStop1.to_excel(writer,'First_Stop') # sheet 2
    CheckLastStop1.to_excel(writer,'Last_Stop') # sheet 3
    writer.save()
    return()
###################################################################################################################################

# GetSummaryGTFSdata
###################################################################################################################################
def GetSummaryGTFSdata(FinDat_, SumDat_,DatFirstLastStops_):
    #TODO: Write Documentation
    '''
    Parameters
    ----------
    FinDat_ : TYPE
        DESCRIPTION.
    SumDat_ : TYPE
        DESCRIPTION.
    DatFirstLastStops_ : 
    Returns
    -------
    None.

    '''
    #5 Get summary after using GTFS data
    ########################################################################################
    FinDat_ = FinDat_.merge(DatFirstLastStops_,on=['filename','IndexTripStartInCleanData'],how='right')
    #Debug : 
    FinDat_[~FinDat_.duplicated(['filename','IndexTripStartInCleanData'])] 
    FinDat_ = FinDat_.query('IndexLoc>=LowerBoundLoc & IndexLoc<=UpperBoundLoc')
    check = FinDat_[['filename','IndexTripStartInCleanData','distFromFirstStop']][~FinDat_.duplicated(['filename','IndexTripStartInCleanData'])] 
    check =check.merge(DatFirstLastStops_,on=['filename','IndexTripStartInCleanData'],how='right')
    check  = check[check.distFromFirstStop_x.isna()]
    assert(check.shape[0]==0)
    FinDat_.rename(columns= {'distFromFirstStop':'Dist_from_GTFS1stStop'},inplace=True)
    FinDat_ = FinDat_[['filename','IndexTripStartInCleanData','Lat','Long','Heading','OdomtFt','SecPastSt'
                           ,'Dist_from_GTFS1stStop']]
    Map1 = lambda x: max(x)-min(x)
    SumDat1 =FinDat_.groupby(['filename','IndexTripStartInCleanData']).agg({'OdomtFt':['min','max',Map1],
                                                     'SecPastSt':['min','max',Map1],
                                                     'Lat':['first','last'],
                                                     'Long':['first','last'],
                                                     'Dist_from_GTFS1stStop':['first']})
    SumDat1.columns = ['StartOdomtFtGTFS','EndOdomtFtGTFS','TripDistMiGTFS',
                       'StartSecPastStGTFS','EndSecPastStGTFS','TripDurSecGTFS',
                       'StartLatGTFS','EndLatGTFS','StartLongGTFS','EndLongGTFS',
                       'StartDistFromGTFS1stStopFt']
    SumDat1.loc[:,['TripDistMiGTFS']] =SumDat1.loc[:,['TripDistMiGTFS']]/5280
    SumDat1.loc[:,'TripSpeedMphGTFS'] =round(3600* SumDat1.TripDistMiGTFS/SumDat1.TripDurSecGTFS,2)
    SumDat1.loc[:,['TripDistMiGTFS','StartDistFromGTFS1stStopFt']] = \
            round(SumDat1.loc[:,['TripDistMiGTFS','StartDistFromGTFS1stStopFt']],2)
    SumDat1 = SumDat1.merge(SumDat_,on=['filename','IndexTripStartInCleanData'],how='left')
    return(SumDat1)
###################################################################################################################################

# mergeStopsGTFSrawnav
###################################################################################################################################
def mergeStopsGTFSrawnav(StopsGTFS, rawnavDat, useAllStopId=False):
    '''
    Parameters
    ----------
    StopsGTFS : pd.DataFrame
        GTFS data with unique stops per route irrespective of short/long or direction.
    rawnavDat : pd.DataFrame
        rawnav data.

    Returns
    -------
    NearestRawnavOnGTFS : gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the GTFS stops on that route. 
    '''
    # Convert to geopandas dataframe
    geometryStops = [Point(xy) for xy in zip(StopsGTFS.stop_lon.astype(float), StopsGTFS.stop_lat.astype(float))]
    geometryPoints = [Point(xy) for xy in zip(rawnavDat.Long.astype(float), rawnavDat.Lat.astype(float))]
    gdA =gpd.GeoDataFrame(StopsGTFS, geometry=geometryStops,crs={'init':'epsg:4326'})
    gdB =gpd.GeoDataFrame(rawnavDat, geometry=geometryPoints,crs={'init':'epsg:4326'})
    # Project to 2-D plane
    # TODO : check if we actually need to convert to a 2-D or the distance in degree can be used.
    #https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    gdA.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    gdB.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    if useAllStopId:
        TripGroups = gdB.groupby(['filename','IndexTripStartInCleanData','route','all_stops']) # Group rawnav data
        GTFS_groups = gdA.groupby(['route_id','all_stops']) # Group GTFS data       
    else:
        TripGroups = gdB.groupby(['filename','IndexTripStartInCleanData','route']) # Group rawnav data
        GTFS_groups = gdA.groupby('route_id') # Group GTFS data
    NearestRawnavOnGTFS =pd.DataFrame()
    for name, groupRawnav in TripGroups:
        #print(name)
        if useAllStopId:
            GTFS_RelevantRouteDat = GTFS_groups.get_group((name[2],name[3])) #Get the relevant group in GTFS corresponding to rawnav.
        else:
            GTFS_RelevantRouteDat = GTFS_groups.get_group(name[2]) #Get the relevant group in GTFS corresponding to rawnav.
            # TODO : Does the GTFS route_id matches exactly with rawnav route? 
        NearestRawnavOnGTFS = pd.concat([NearestRawnavOnGTFS,\
                                         ckdnearest(GTFS_RelevantRouteDat,groupRawnav)])
    NearestRawnavOnGTFS.dist = NearestRawnavOnGTFS.dist * 3.28084 # meters to feet
    NearestRawnavOnGTFS.Lat =NearestRawnavOnGTFS.Lat.astype('float')
    # 2nd method for cross checking
    #NearestRawnavOnGTFS.loc[:,"CheckDist"] = NearestRawnavOnGTFS.apply(lambda x: geopy.distance.geodesic((x.Lat,x.Long),(x.stop_lat,x.stop_lon)).meters,axis=1)
    # TODO : Remove later. Might be consuming unnecessary resources. 
    geometry1 = [Point(xy) for xy in zip(NearestRawnavOnGTFS.Long, NearestRawnavOnGTFS.Lat)] 
    geometry2 = [Point(xy) for xy in zip(NearestRawnavOnGTFS.stop_lon, NearestRawnavOnGTFS.stop_lat)]
    geometry = [LineString(list(xy)) for xy in zip(geometry1,geometry2)]
    NearestRawnavOnGTFS=gpd.GeoDataFrame(NearestRawnavOnGTFS, geometry=geometry,crs={'init':'epsg:4326'})
    NearestRawnavOnGTFS.rename(columns = {'dist':'distNearestPointFromStop'},inplace=True)
    return(NearestRawnavOnGTFS)
###################################################################################################################################

# ckdnearest
###################################################################################################################################
#https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
def ckdnearest(gdA, gdB):
    #TODO: Write Documentation
    '''
    Parameters
    ----------
    gdA : TYPE
        DESCRIPTION.
    gdB : TYPE
        DESCRIPTION.

    Returns
    -------
    gdf : TYPE
        DESCRIPTION.
    '''
    gdA.reset_index(inplace=True);gdB.reset_index(inplace=True)
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)) )
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)) )
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdf = pd.concat(
        [gdA.reset_index(drop=True), gdB.loc[idx, ['filename','IndexTripStartInCleanData','IndexLoc','Lat', 'Long']].reset_index(drop=True),
         pd.Series(dist, name='dist')], axis=1)
    return gdf
###################################################################################################################################

# GetCorrectDirGTFS
###################################################################################################################################
def GetCorrectDirGTFS(Dat1stStop,DatLastStop,SumDat_,routeNoUnique1stStp):
    #TODO: Write Documentation
    # Get Correct Direction
    #######################################################
    # Tried 200 ft. but it is causing incorrect matching. Need to use a big number.
    # 
    OneMi = 5280
    Dat1stStop = Dat1stStop.query('distNearestPointFromStop<@OneMi') # Get trips that have the closest rawnav point within 1 mi. of 1nd stop
    Dat1stStop.sort_values(['filename','IndexTripStartInCleanData',
                                      'direction_id','stop_sequence'],inplace=True)
    Dat1stStop.loc[:,"tempCol"]= Dat1stStop.groupby(['filename','IndexTripStartInCleanData']).IndexLoc.transform(min)
    Dat1stStop = Dat1stStop.query('IndexLoc==tempCol').reset_index(drop=True).drop(columns='tempCol')
    DatProbRoutes = Dat1stStop.query('route_id in @routeNoUnique1stStp')
    Dat1stStop = Dat1stStop.query('route_id not in @routeNoUnique1stStp')
    assert(Dat1stStop.duplicated(['filename','IndexTripStartInCleanData']).sum()==0)
    ############################################################
    DatLastStop = DatLastStop[['filename','IndexTripStartInCleanData','direction_id','IndexLoc','distNearestPointFromStop','all_stops','trip_id','stop_sequence']]
    DatLastStop.rename(columns={'IndexLoc':'IndexLocLastStop','distNearestPointFromStop':'distLastStop'},inplace=True)
    DatLastStop = DatLastStop.query('distLastStop<@OneMi') # Get trips that have the closest rawnav point within 1 mi. of 1nd stop
    DatLastStop.sort_values(['filename','IndexTripStartInCleanData',
                                      'direction_id','stop_sequence'],inplace=True)
    DatLastStop.loc[:,"tempCol"]= DatLastStop.groupby(['filename','IndexTripStartInCleanData']).IndexLocLastStop.transform(max)
    DatLastStop = DatLastStop.query('IndexLocLastStop==tempCol').reset_index(drop=True).drop(columns='tempCol')

    DatLastStop = DatLastStop[['filename','IndexTripStartInCleanData','direction_id','all_stops','IndexLocLastStop']]
    # Inner merge to find trips where we got a match in Dat1stStop and Dat1stStop: uniquely identify a trip
    DatProbRoutes =DatProbRoutes.merge(DatLastStop, on=['filename','IndexTripStartInCleanData','direction_id','all_stops'],how='inner')
    assert(DatProbRoutes.duplicated(['filename','IndexTripStartInCleanData']).sum()==0)
    Dat1stStop= pd.concat([Dat1stStop,DatProbRoutes])
    print(f"Removed {SumDat_.shape[0] - Dat1stStop.shape[0]} trips from {SumDat_.shape[0]}")
    Dat1stStop = Dat1stStop[['filename', 'IndexTripStartInCleanData','direction_id','stop_name','all_stops']]
    Dat1stStop.rename(columns={'stop_name':'1st_stopNm'},inplace=True)
    SumDat_.loc[:,"tempIndex"] = SumDat_.filename+SumDat_.IndexTripStartInCleanData.astype(str)
    # Debug data:
    # tempIndex = Dat1stStop.filename+Dat1stStop.IndexTripStartInCleanData.astype(str)
    # tempDat = SumDat_.query('tempIndex not in @tempIndex')
    return(Dat1stStop)
###################################################################################################################################    

# FindStopOnRoute
###################################################################################################################################
def FindStopOnRoute(DatStopSnapping, SumDat_):
    # Find stops on route
    #######################################################
    DatStopSnappingTemp = DatStopSnapping.copy()
    print(f"Removing {DatStopSnapping.query('distNearestPointFromStop>200').shape[0]} from {DatStopSnapping.shape[0]} stops/ rows where the nearest rawnav point from a stop is over 200 ft.")
    # Remove long route stops from a short route trip
    DatStopSnapping =DatStopSnapping.query('distNearestPointFromStop<200') # Remove stops where nearest rawnav point is more than 200 ft. away
    DatStopSnapping.sort_values(['filename','IndexTripStartInCleanData',
                                      'direction_id','stop_sequence'],inplace=True)
    DatStopSnapping.loc[:,"tempCol"]= DatStopSnapping.groupby(['filename','IndexTripStartInCleanData','stop_sequence']).IndexLoc.transform(min)

    #Tie breaker for short and long route which where a given stop sequence can be for 2 different stop locations
    #Inefficient method: DatStopSnapping = DatStopSnapping.groupby(['filename','IndexTripStartInCleanData','stop_sequence']).apply(lambda  g: g[g['IndexLoc'] == g['IndexLoc'].min()]).reset_index(drop=True)
    DatStopSnapping.loc[:,"tempCol"]= DatStopSnapping.groupby(['filename','IndexTripStartInCleanData','stop_sequence']).IndexLoc.transform(min)
    DatStopSnapping = DatStopSnapping.query('IndexLoc==tempCol').reset_index(drop=True).drop(columns='tempCol')
    #Tie breaker for very close bus stops which get snapped to the same rawnav point
    DatStopSnapping.loc[:,"tempCol"]= DatStopSnapping.groupby(['filename','IndexTripStartInCleanData','stop_sequence']).distNearestPointFromStop.transform(min)
    DatStopSnapping = DatStopSnapping.query('distNearestPointFromStop==tempCol').reset_index(drop=True).drop(columns='tempCol')
    check =DatStopSnapping[DatStopSnapping.duplicated(['filename','IndexTripStartInCleanData','stop_sequence'],keep=False)]
    assert(DatStopSnapping.duplicated(['filename','IndexTripStartInCleanData','stop_sequence']).sum()==0), "Unique stop sequence; handle short and long route"
    DatStopSnapping.sort_values(['filename','IndexTripStartInCleanData',
                                      'direction_id','stop_sequence'],inplace=True)
    try:
        assert(sum(DatStopSnapping.groupby(['filename','IndexTripStartInCleanData']).IndexLoc.diff().dropna()<=0)==0)
    except AssertionError as AsErr:
        print("Gathering trips with incorrect direction issue.")
        issueDat = DatStopSnapping[DatStopSnapping.groupby(['filename','IndexTripStartInCleanData']).IndexLoc.diff()<=0]
        issueDat.sort_values(['filename','IndexTripStartInCleanData','stop_sequence'],inplace=True)
        Mask= issueDat[['filename','IndexTripStartInCleanData']].duplicated(['filename','IndexTripStartInCleanData'])
        issueDat1 = issueDat.loc[~Mask,['filename','IndexTripStartInCleanData']]
        issueDat1.loc[:,'delCol'] = True
        DatStopSnapping =DatStopSnapping.merge(issueDat1,how='left')
        DatStopSnapping = DatStopSnapping.query('delCol!=True').drop(columns='delCol')
        issueDat1 = issueDat1.merge(DatStopSnappingTemp)
        issueDat1.sort_values(['filename','IndexTripStartInCleanData','stop_sequence'],inplace=True)

    DatLastStop = DatStopSnapping
    DatLastStop.loc[:,"tempCol"]= DatStopSnapping.groupby(['filename','IndexTripStartInCleanData']).stop_sequence.transform(max)
    DatLastStop = DatLastStop.query('stop_sequence==tempCol').reset_index(drop=True).drop(columns='tempCol')
    
    DatLastStop = DatLastStop[['filename','IndexTripStartInCleanData','IndexLoc',
                                'distNearestPointFromStop','stop_sequence','stop_name',
                                'stop_lat','stop_lon']].\
        rename(columns={'IndexLoc':'UpperBoundLoc','distNearestPointFromStop':"distFromLastStop",
                        'stop_sequence':'lastStopSequence','stop_name':'last_stopNm',
                        'stop_lat':'LastStop_lat','stop_lon':'LastStop_lon'})
    # DatFirstStop =  DatStopSnapping.groupby(['filename', 'IndexTripStartInCleanData']).\
    #     apply(lambda g: g[g['stop_sequence'] == g['stop_sequence'].min()]).reset_index(drop=True)        
    # TODO: change apply to other efficient function
    DatFirstStop = DatStopSnapping
    DatFirstStop.loc[:,"tempCol"]= DatFirstStop.groupby(['filename','IndexTripStartInCleanData']).stop_sequence.transform(min)
    DatFirstStop = DatFirstStop.query('stop_sequence==tempCol').reset_index(drop=True).drop(columns='tempCol')
    DatFirstStop.rename(columns ={'IndexLoc':"LowerBoundLoc",'distNearestPointFromStop':"distFromFirstStop",
                                  'stop_sequence':'firstStopSequence','stop_lat':'StartStop_lat',
                                  'stop_lon':'StartStop_lon','stop_name':'first_stopNm'},inplace=True)
    DatFirstStop.drop(columns=['Lat', 'Long'],inplace=True)
    DatFirstLastStops = DatFirstStop.merge(DatLastStop,on=['filename','IndexTripStartInCleanData'],how='left')
    DatFirstLastStops.drop(columns=['geometry'],inplace=True)
    print(f'Total number of trips/ rows removed during cleaning : {SumDat_.shape[0]-DatFirstLastStops.shape[0]}')
    return(DatStopSnapping,DatFirstLastStops, issueDat)
###################################################################################################################################

# PlotRawnavTrajWithGTFS
###################################################################################################################################
def PlotRawnavTrajWithGTFS(RawnavTraj, GTFScloseStop):
    #TODO: Write Documentation
    '''
    Parameters
    ----------
    RawnavTraj : TYPE
        DESCRIPTION.
    GTFScloseStop : TYPE
        DESCRIPTION.
    path_processed_data_ : TYPE
        DESCRIPTION.
    SaveFileNm : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    ## Link to Esri World Imagery service plus attribution
    #https://www.esri.com/arcgis-blog/products/constituent-engagement/constituent-engagement/esri-world-imagery-in-openstreetmap/
    EsriImagery = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
    EsriAttribution = "Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community"
    this_map = folium.Map( tiles='cartodbdark_matter', zoom_start=16,max_zoom=25,control_scale=True)
    folium.TileLayer(name="EsriImagery",tiles=EsriImagery, attr=EsriAttribution, zoom_start=16,max_zoom=25,control_scale=True).add_to(this_map)
    folium.TileLayer('cartodbpositron',zoom_start=16,max_zoom=20,control_scale=True).add_to(this_map)
    folium.TileLayer('openstreetmap',zoom_start=16,max_zoom=20,control_scale=True).add_to(this_map) 

    fg = folium.FeatureGroup(name="Rawnav Trajectory")
    this_map.add_child(fg)
    LineGr = folium.FeatureGroup(name="GTFS Stops and Nearest Rawnav Point")
    this_map.add_child(LineGr)
    #StpGr = folium.FeatureGroup(name="GTFS Stops")
    #this_map.add_child(StpGr)
    #PlotMarkerClusters(this_map, GTFScloseStop,"stop_lat","stop_lon",StpGr)
    PlotMarkerClusters(this_map, RawnavTraj,"Lat","Long",fg)
    GTFScloseStop.sort_values(['stop_sequence'])
    PlotLinesClusters(this_map, GTFScloseStop,LineGr)
    LatLongs = [[x,y] for x,y in zip(RawnavTraj.Lat,RawnavTraj.Long)]
    this_map.fit_bounds(LatLongs)
    folium.LayerControl(collapsed=True).add_to(this_map)
    return(this_map)
###################################################################################################################################

# PlotMarkerClusters
###################################################################################################################################
def PlotMarkerClusters(this_map, Dat,Lat,Long, FeatureGrp):
    #TODO: Write Documentation
    '''
    Parameters
    ----------
    this_map : TYPE
        DESCRIPTION.
    Dat : TYPE
        DESCRIPTION.
    Lat : TYPE
        DESCRIPTION.
    Long : TYPE
        DESCRIPTION.
    FeatureGrp : TYPE
        DESCRIPTION.
    Returns
    -------
    None.
    '''
    popup_field_list = list(Dat.columns)     
    for i,row in Dat.iterrows():
        label = '<br>'.join([field + ': ' + str(row[field]) for field in popup_field_list])
        #https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
        folium.CircleMarker(
                location=[row[Lat], row[Long]], radius= 2,
                popup=folium.Popup(html = label,parse_html=False,max_width='200')).add_to(FeatureGrp)
###################################################################################################################################

# PlotLinesClusters
###################################################################################################################################
def PlotLinesClusters(this_map, Dat, FeatureGrp):
    #TODO: Write Documentation
    '''
    Parameters
    ----------
    this_map : TYPE
        DESCRIPTION.
    Dat : TYPE
        DESCRIPTION.
    FeatureGrp : TYPE
        DESCRIPTION.
    Returns
    -------
    None.
    '''
    popup_field_list = list(Dat.columns)     
    popup_field_list.remove('geometry')
    for i,row in Dat.iterrows():
        TempGrp = plugins.FeatureGroupSubGroup(FeatureGrp,f"{row.stop_sequence}-{row.stop_name}-{row.direction_id}")
        this_map.add_child(TempGrp)
        label = '<br>'.join([field + ': ' + str(row[field]) for field in popup_field_list])
        #https://deparkes.co.uk/2019/02/27/folium-lines-and-markers/
        LinePoints = [(tuples[1],tuples[0]) for tuples in list(row.geometry.coords)]
        folium.PolyLine(LinePoints, color="red", weight=4, opacity=1\
        ,popup=folium.Popup(html = label,parse_html=False,max_width='300')).add_to(TempGrp)
###################################################################################################################################

###################################################################################################################################
###################################################################################################################################   
    
    
    
    

