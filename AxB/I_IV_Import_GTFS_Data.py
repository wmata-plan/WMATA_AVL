# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 12:35:10 2020

@author: abibeka
"""

#0.0 Housekeeping. Clear variable space
# from IPython import get_ipython  #run magic commands
# ipython = get_ipython()
# ipython.magic("reset -f")
# ipython = get_ipython()

#1 Import Libraries
########################################################################################
import pandas as pd, os, numpy as np, pyproj

#2 Read the Data
########################################################################################
os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data')
GTFS_Dir = "./google_transit"
StopsDat= pd.read_csv(os.path.join(GTFS_Dir,"stops.txt"))
StopTimeDat = pd.read_csv(os.path.join(GTFS_Dir,"stop_times.txt"))
TripsDat =pd.read_csv(os.path.join(GTFS_Dir,"trips.txt"))
StopsDat= StopsDat[['stop_id','stop_name','stop_lat','stop_lon']]
StopTimeDat = StopTimeDat[['trip_id','arrival_time','departure_time','stop_id','stop_sequence','pickup_type','drop_off_type']]
TripsDat = TripsDat[['route_id','service_id','trip_id','trip_headsign','direction_id']]
with pd.option_context('display.max_rows', 5, 'display.max_columns', 10):
    display(StopTimeDat)

#3 Analyze the Trip Start and End
########################################################################################
#trip_id is a unique identifier irrespective of the route
TripsDat.trip_id = TripsDat.trip_id.astype(str)
StopTimeDat.trip_id = StopTimeDat.trip_id.astype(str)
# 0: travel in one direction; 1 travel in opposite direction
TripSumDa = TripsDat.groupby(['route_id','direction_id','trip_headsign']).count().reset_index()
Merdat = TripsDat.merge(StopTimeDat,on="trip_id",how='inner')
Merdat = Merdat.merge(StopsDat,on= "stop_id")

FirstStopDat = Merdat.groupby(['route_id','direction_id','trip_headsign']).first()



LastStopDat = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index()







#4 Subset Data for Route 79
########################################################################################

TripsDat = TripsDat[TripsDat.route_id=="79"]
TripsDat.trip_id = TripsDat.trip_id.astype(str)
StopTimeDat.trip_id = StopTimeDat.trip_id.astype(str)


TripsDat.trip_headsign.value_counts()
TripsDat.groupby(['trip_headsign','direction_id']).count()
# 0: travel in one direction; 1 travel in opposite direction

Merdat = TripsDat.merge(StopTimeDat,on="trip_id",how='inner')
NumStops = Merdat.groupby(['trip_id','direction_id'])['stop_id'].count()
NumStops.describe()


Merdat = Merdat.merge(StopsDat,on= "stop_id")

#Make Sure all trips have same 1st stop
FirstStopDat = Merdat[Merdat.stop_sequence==1].reset_index()
assert(np.var(FirstStopDat.stop_sequence)==0)
assert(FirstStopDat.groupby(['route_id','direction_id'])['stop_id'].var().sum()==0)
FirstStopDat = FirstStopDat.groupby(['route_id','direction_id']).first()
#Make sure all trips have same end stop
LastStopDat = Merdat.groupby('trip_id')['stop_sequence'].max().reset_index()
LastStopDat = LastStopDat.merge(Merdat, on=['trip_id','stop_sequence'],how='left')
assert(np.var(LastStopDat.stop_sequence)==0)
assert(LastStopDat.groupby(['route_id','direction_id'])['stop_id'].var().sum()==0)
LastStopDat = LastStopDat.groupby(['route_id','direction_id']).first()
#The above assertions would start failing for routes like S9; routes with long and short routes
FirstStopDat.columns
FirstStopDat.rename(columns = {'stop_id':'first_stopId','stop_name':"first_stopNm",'stop_lat':'first_sLat',
                               'stop_lon':'first_sLon',
                               'trip_headsign':'end_stop_desc'},inplace=True)
FirstStopDat = FirstStopDat[['first_stopId',"first_stopNm",'first_sLat','first_sLon','end_stop_desc']]
LastStopDat.rename(columns = {'stop_id':'last_stopId','stop_name':"last_stopNm",'stop_lat':'last_sLat',
                               'stop_lon':'last_sLon'},inplace=True)
LastStopDat = LastStopDat[['last_stopId',"last_stopNm",'last_sLat','last_sLon']]

FinStopDat= pd.merge(FirstStopDat,LastStopDat,left_index=True,right_index=True)
FinStopDat.reset_index(inplace=True)

geodesic = pyproj.Geod(ellps='WGS84')
fwd_azimuth,back_azimuth,distance = geodesic.inv(FinStopDat.first_sLat[0], FinStopDat.first_sLon[0], 
                                                 FinStopDat.last_sLat[0], FinStopDat.last_sLon[0])
fwd_azimuth,back_azimuth,distance = geodesic.inv(FinStopDat.first_sLat[1], FinStopDat.first_sLon[1], 
                                                 FinStopDat.last_sLat[1], FinStopDat.last_sLon[1])
#Need to find a way to determine direction of the route
# direction_id : 0,1 might not always mean outbound and inbound



FinStopDat.to_csv("./StopDetails.csv",index=False)


