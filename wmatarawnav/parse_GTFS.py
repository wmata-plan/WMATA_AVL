# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 15:07:59 2020

@author: 
"""
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
from shapely.ops import nearest_points

def readGTFS(GTFS_Dir_):
    StopsDat= pd.read_csv(os.path.join(GTFS_Dir_,"stops.txt"))
    StopTimeDat = pd.read_csv(os.path.join(GTFS_Dir_,"stop_times.txt"))
    TripsDat =pd.read_csv(os.path.join(GTFS_Dir_,"trips.txt"))
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


def get1ststop(Merdat,AnalysisRoutes_):
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
    
    #4 Get first stops and lasts stops by route---ignore direction
    ########################################################################################
    dropCols = ['direction_id','trip_headsign','arrival_time','departure_time']
    FirstStopDat1 =FirstStopDat.drop(columns=dropCols)        
    FirstStopDat1 = FirstStopDat1.groupby(['route_id','first_stopId','first_stopNm']).first()
    FirstStopDat1.reset_index(inplace=True)
    FirstStopDat1_rte = FirstStopDat1.query('route_id in @AnalysisRoutes_') 
    def to_set(x):
        return set(x)
    CheckFirstStop = FirstStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'first_stopId':to_set,'first_stopNm':to_set})
    CheckFirstStop1 = FirstStopDat.groupby(['route_id','direction_id','trip_headsign',\
                                            'first_stopId','first_stopNm'])\
                                            .agg({'first_sLat':['count','first'],'first_sLon':'first'
                                                 ,'arrival_time':to_set,'departure_time':to_set})
    return(FirstStopDat1_rte, CheckFirstStop, CheckFirstStop1)

def getlaststop(Merdat,AnalysisRoutes_):
    #Get Last stops per trip
    #########################################
    TempDa = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index() #can use groupby filter here
    LastStopDat = TempDa.merge(Merdat, on=['trip_id','stop_sequence'],how='left')
    LastStopDat.rename(columns = {'stop_id':'last_stopId','stop_name':"last_stopNm",'stop_lat':'last_sLat',
                                   'stop_lon':'last_sLon'},inplace=True)
    LastStopDat = LastStopDat[['route_id','direction_id','trip_headsign',
                               'last_stopId',"last_stopNm",'last_sLat','last_sLon','arrival_time','departure_time']]
    #4 Get lasts stops by route---ignore direction
    ########################################################################################
    dropCols = ['direction_id','trip_headsign','arrival_time','departure_time']
    LastStopDat1 =LastStopDat.drop(columns=dropCols)        
    LastStopDat1 = LastStopDat1.groupby(['route_id','last_stopId','last_stopNm']).first()
    LastStopDat1.reset_index(inplace=True)
    LastStopDat1_rte = LastStopDat1.query('route_id in @AnalysisRoutes_') 
    def to_set(x):
        return set(x)
    CheckLastStop = LastStopDat.groupby(['route_id','direction_id','trip_headsign']).agg({'last_stopId':to_set,'last_stopNm':to_set})
    CheckLastStop1 = LastStopDat.groupby(['route_id','direction_id','trip_headsign',
                                          'last_stopId','last_stopNm'])\
                                        .agg({'last_sLat':['count','first'],
                                              'last_sLon':'first' ,'arrival_time':to_set,'departure_time':to_set})
    return(LastStopDat1_rte, CheckLastStop, CheckLastStop1)
            
                                        
def debugGTFS1stLastStopData(CheckFirstStop,CheckFirstStop1,CheckLastStop,CheckLastStop1,path_processed_data_):
    First_Last_Stop = CheckFirstStop.merge(CheckLastStop,left_index=True,right_index=True,how='left')
    #Check stops Data
    #########################################
    OutFi = os.path.join(path_processed_data_,'First_Last_Stop.xlsx')
    writer = pd.ExcelWriter(OutFi)
    First_Last_Stop.to_excel(writer,'First_Last_Stop')
    CheckFirstStop1.to_excel(writer,'First_Stop')
    CheckLastStop1.to_excel(writer,'Last_Stop')
    writer.save()
    return()



def getNearestStartEnd(SumDatStart, SumDatEnd, FirstStopDat1_rte, LastStopDat1_rte, AnalysisRoutes_):
    FirstStp = pd.DataFrame()
    LastStp = pd.DataFrame()
    TempDict = {}
    for rte in AnalysisRoutes_:
        FirstStp = FirstStopDat1_rte.query('route_id == @rte')
        LastStp = LastStopDat1_rte.query('route_id == @rte')
        geometryStart = [Point(xy) for xy in zip(FirstStp.first_sLon, FirstStp.first_sLat)]
        FirstStp=gpd.GeoDataFrame(FirstStp, geometry=geometryStart,crs={'init':'epsg:4326'})
        geometryEnd = [Point(xy) for xy in zip(LastStp.last_sLon, LastStp.last_sLat)]
        LastStp=gpd.GeoDataFrame(LastStp, geometry=geometryEnd,crs={'init':'epsg:4326'})
        TempDict[rte] = {'FirstStp':FirstStp,'LastStp':LastStp}
    #https://stackoverflow.com/questions/56520780/how-to-use-geopanda-or-shapely-to-find-nearest-point-in-same-geodataframe
    try:
        SumDatStart.insert(3, 'nearest_start', None)
        SumDatEnd.insert(3, 'nearest_end', None)
    except: pass
    for index, row in SumDatStart.iterrows():
        point = row.geometry
        multipoint = TempDict[row.route]['FirstStp'].geometry.unary_union
        queried_geom, nearest_geom = nearest_points(point, multipoint)
        SumDatStart.loc[index, 'nearest_start'] = nearest_geom
    for index, row in SumDatEnd.iterrows():
        point = row.geometry
        multipoint = TempDict[row.route]['LastStp'].geometry.unary_union
        queried_geom, nearest_geom = nearest_points(point, multipoint)
        SumDatEnd.loc[index, 'nearest_end'] = nearest_geom
    SumDatStart = SumDatStart[['filename','route','IndexTripStartInCleanData','nearest_start']]
    SumDatEnd = SumDatEnd[['filename','route','IndexTripStartInCleanData','nearest_end']]
    return(SumDatStart, SumDatEnd)


def GetSummaryGTFSdata(FinDat_, SumDat_):
    #5 Get summary after using GTFS data
    ########################################################################################
    MinDat = FinDat_.groupby(['filename','IndexTripStartInCleanData'])['distances_start_ft','distances_end_ft'].idxmin().reset_index()
    MinDat.rename(columns = {'distances_start_ft':'LowerBoundLoc','distances_end_ft':"UpperBoundLoc"},inplace=True)
    MinDat.loc[:,'LowerBound'] = FinDat_.loc[MinDat.loc[:,'LowerBoundLoc'],'IndexLoc'].values
    MinDat.loc[:,'UpperBound'] = FinDat_.loc[MinDat.loc[:,'UpperBoundLoc'],'IndexLoc'].values
    MinDat.drop(columns=['LowerBoundLoc','UpperBoundLoc'],inplace=True)
    MinDat.reset_index(inplace=True)
    FinDat_ = FinDat_.merge(MinDat,on=['filename','IndexTripStartInCleanData'],how='left')
    FinDat_ = FinDat_.query('IndexLoc>=LowerBound & IndexLoc<=UpperBound')
    FinDat_.rename(columns= {'distances_start_ft':'Dist_from_GTFS1stStop',
                                  'distances_end_ft':'Dist_from_GTFSlastStop'},inplace=True)
    FinDat_ = FinDat_[['filename','IndexTripStartInCleanData','Lat','Long','Heading','OdomtFt','SecPastSt'
                           ,'Dist_from_GTFS1stStop','Dist_from_GTFSlastStop']]
    
    Map1 = lambda x: max(x)-min(x)
    SumDat1 =FinDat_.groupby(['filename','IndexTripStartInCleanData']).agg({'OdomtFt':['min','max',Map1],
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
    SumDat1 = SumDat1.merge(SumDat_,on=['filename','IndexTripStartInCleanData'],how='left')
    return(SumDat1)



#https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
# unary union of the gpd2 geomtries 
pts3 = gpd2.geometry.unary_union
def near(point, pts=pts3):
     # find the nearest point and return the corresponding Place value
     nearest = gpd2.geometry == nearest_points(point, pts)[1]
     return gpd2[nearest].Place.get_values()[0]
gpd1['Nearest'] = gpd1.apply(lambda row: near(row.geometry), axis=1)











