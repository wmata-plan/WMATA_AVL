# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 12:20:37 2020

@author: abibeka
"""

#Cut Trips using GTFS data
##############################################################################################################################
TestDat1 = ProcessedRawDataDict['06431190501']
TestDat1.columns
TestDat1.loc[:,"route_id"] = TestDat1.Tag.apply(lambda x: x[0:2])
TestDat1.loc[:,"direction_id"] = -999
TestDat1.loc[TestDat1.route_id=="79",'direction_id'] = TestDat1.loc[TestDat1.route_id=="79",'Tag'].apply(lambda x: x[2:4])
TestDat1.direction_id = TestDat1.direction_id.astype(int)
sum(TestDat1.route_id=="79")
def DirectionNm79(x):
    retDir = ""
    if(x==1):
        retDir = "inbound"
    elif(x==2):
        retDir = "outbound"
    else:
        retDir = ""
    return(retDir)
TestDat1.loc[:,"dir_Nm"] = TestDat1.direction_id.apply(DirectionNm79)
TestDat1.dir_Nm.value_counts()
os.getcwd()
stopData = pd.read_csv('StopDetails.csv')
def Gtfs_DirectionNm79(x):
    retDir = ""
    if(x==1):
        retDir = "inbound"
    elif(x==0):
        retDir = "outbound"
    else:
        retDir = ""
    return(retDir)
stopData.loc[:,"dir_Nm"] = stopData.direction_id.apply(Gtfs_DirectionNm79)
stopData.route_id =stopData.route_id.astype(str)
stopData.dir_Nm.value_counts()
stopData.set_index("dir_Nm",inplace=True) 
stopDataDict = stopData.to_dict(orient='index')
sum(stopData.route_id=="79")

# TestDat1 = TestDat1.merge(stopData, on =['route_id',"dir_Nm"],how="left")
TestDat1[TestDat1.route_id=="79"]
TestDat1.columns


# Use Geopy library --- Pyorg distance units are not clear
def GetDistanceFromStart_Rt79(row,StopDict):
    distance_miles = -999
    if row.dir_Nm in(['inbound','outbound']):
        lat1 = row['Lat']; long1 = row['Long']
        lat2 = StopDict[row.dir_Nm]['first_sLat']; long2 = StopDict[row.dir_Nm]['first_sLon']
        distance_feets = geodesic((lat1, long1), (lat2, long2)).feets
    return(distance_feets)

def GetDistanceFromEnd_Rt79(row, StopDict):
    distance_miles = -999
    if row.dir_Nm in(['inbound','outbound']):
        lat1 = row['Lat']; long1 = row['Long']
        lat2 = StopDict[row.dir_Nm]['last_sLat']; long2 = StopDict[row.dir_Nm]['last_sLon']
        distance_feets = geodesic((lat1, long1), (lat2, long2)).feets
    return(distance_feets)

TestDat1.loc[:,"Dist_from_1stStop"] = TestDat1.apply(lambda x: GetDistanceFromStart_Rt79(x,stopDataDict), axis=1) 
TestDat1.loc[:,"Dist_from_lastStop"] = TestDat1.apply(lambda x: GetDistanceFromEnd_Rt79(x,stopDataDict), axis=1) 

TestDat1.Dist_from_1stStop.describe()
TestDat1.Dist_from_lastStop.describe()

CheckDat1 = TestDat1[(TestDat1.Dist_from_1stStop < 2000)&(TestDat1.Dist_from_1stStop > 0)]
CheckDat1.set_index(['Tag','IndexTripTags','IndexLoc'],inplace=True)
CheckDat1.columns
CheckDat1 = CheckDat1[[ 'Lat', 'Long', 'Heading', 'DoorState', 'VehState',
       'OdomtFt', 'SecPastSt', 'SatCnt', 'StopWindow','dir_Nm', 'Dist_from_1stStop',
       'Dist_from_lastStop']]
CheckDat1.to_excel("./ProcessedData/Sample_Route79_Stop_2000ft.xlsx")