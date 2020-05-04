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
from geopy.distance import geodesic
from collections import defaultdict

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
restrict_n = 10000
# restrict_n = None

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
#Directly get the zipped files path
# WT: i'm a little confused by this; we're calling this function twice with a 
#    different argument and reassigning file universe?
#AxB: The function GetZippedFilesFromZipDir() extracts the zipped folder and gets the list of files 
# from the unzipped folder. It does this if the Unzipped folder does not exist. The following function
# call is just to show that the user can also use the Unzipped folder. 
#FileUniverse = wr.GetZippedFilesFromZipDir(UnZippedFilesDir,ZippedFilesDirParent) 

# Return a dataframe of routes and details
rawnav_inventory = wr.find_rawnav_routes(FileUniverse, nmax = restrict_n, quiet = True)
# Filter to our set of analysis routes and any other conditions
rawnav_inventory_filtered = rawnav_inventory.loc[(rawnav_inventory['route'].isin(AnalysisRoutes))]
# AxB: Still need tag information at file level. Need tags from other routes to define a trip. 
# Will subset data by route later
rawnav_inventory1 = rawnav_inventory[rawnav_inventory.filename.isin(rawnav_inventory_filtered.filename.unique())]
if (len(rawnav_inventory_filtered) ==0):
    raise Exception ("No Analysis Routes found in FileUniverse")

# Return filtered list of files to pass to read-in functions
# WT: Python is weird, any advice on converting column back to character list?
# AxB: Do mean like the output of FindAllTags()?. Try something like
#YourDataFrame.groupby(["CommonKey"])['Column"].apply(list)
Example = rawnav_inventory_filtered.groupby(['fullpath','filename'])['taglist'].apply(list)
# Naming is hard

# rawnav_inv_filt_first = rawnav_inventory_filtered.groupby('fullpath').first().reset_index()
# AxB: One file can have different routes. Just the first line for not work. We need to groupby fullpath and route
# We not seening the issue with routes '79','X2','X9' in the 1st 500 files, but, if you look at route 'U6'and 'H4',
# you will see this issue.
# I am loosing the route information here to read files with multiple routes togather. Will use the rawnav_inventory_filtered
# later to get back the mapping. 
rawnav_inventory1.loc[:,"line_num"] = rawnav_inventory1.line_num.astype(int)
rawnav_inv_filt_first = rawnav_inventory1.groupby(['fullpath','filename']).line_num.min().reset_index()

# 3 Load Raw RawNav Data
########################################################################################
# Data is loaded into a dictionary named by the ID
RouteRawTagDict = {}
for index, row in rawnav_inv_filt_first.iterrows():
    #Since we already parsed the entire file for tag information. We can reuse the tag infomation here.
    #Cannot use rawnav_inventory_filtered. We need all the tag information for defining a trip.
    #Eg. Say we are analyzing route 79, we see a tag that says route 79 starts. The end tag can 
    # Deadheading, PI, S9 etc. 
    tagInfo_LineNo = rawnav_inventory1[rawnav_inventory1['filename'] == row['filename']]
    Refrence = min(tagInfo_LineNo.line_num.astype(int))
    tagInfo_LineNo.loc[:,"NewLineNo"] = tagInfo_LineNo.line_num.astype(int) - Refrence-1
    # Two routes can have the same file
    # FileID gets messy; string to number conversion loose the initial zeros. "filename" is easier to deal with.
    temp = wr.load_rawnav_data(ZipFolderPath = row['fullpath'], skiprows = pd.to_numeric(row['line_num']))   
    RouteRawTagDict[row['filename']] = {'RawData':temp,'tagLineInfo':tagInfo_LineNo}
    
# 4 Clean RawNav Data
########################################################################################
#DEBUG
RouteRawTagDict.keys()
key = 'rawnav02683191011.txt'
rawnavdata =data = RouteRawTagDict[key]['RawData']
taglineData = RouteRawTagDict[key]['tagLineInfo']
#AxB: Stopped here. Figured out the logic though. Will implement tomorrow (Mon: 5/3/2020)
CleanDataDict = {}
for key, datadict in RouteRawTagDict.items():
    # WT: Is there a way we can restructure the clean_rawnav_data to not 
    # require FirstTag to be passed like this? Can we used nested dataframes
    # in a master dataframe with list comprehensions 
    # instead of trying to subset a bunch of different objects?
    # AxB: I changed the above dictionary to include tag information. Wouldn't nested dataframe have 
    # essentially the same information?     
    # WT: Apoorb, I think I'll need your help on GetTagInfo
    # I think the way I'm trying to pass tag isn't quite working there    
    CleanDataDict[key] = wr.clean_rawnav_data(datadict)


# WT: stopped here working on code
breakpoint() 
ZipDirList=ZippedFilesDirs

#5 Analyze Route 79
########################################################################################






#2 Read the GTFS Data
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


    FirstStopLatLong_RawNav = pd.DataFrame()
    LastStopLatLong_RawNav = pd.DataFrame()

    #****************************************************************************************************************
    GTFS_1stStop = FirstStopDat1[FirstStopDat1.route_id==rte_id]
    GTFS_lastStop = LastStopDat1[LastStopDat1.route_id==rte_id]

    #****************************************************************************************************************
    for idx, row in FirstRawNavRow.iterrows():
        RawNavLat1,RawNavLong1 = row[['Lat','Long']]
        GTFS_1stStop.loc[:,'Dist_from_1st_RawNav_Pt'] = GTFS_1stStop.apply(lambda row:wr.GetDistanceLatLong_ft(row['first_sLat'],row['first_sLon'],RawNavLat1,RawNavLong1) ,axis=1)
        MaskClosestStop = GTFS_1stStop.Dist_from_1st_RawNav_Pt== min(GTFS_1stStop.Dist_from_1st_RawNav_Pt )
        TempLat = GTFS_1stStop[MaskClosestStop][['first_sLat','first_sLon']].values[0][0]
        TempLong = GTFS_1stStop[MaskClosestStop][['first_sLat','first_sLon']].values[0][1]
        TempDa2 = pd.DataFrame({'IndexTripTags':[row['IndexTripTags']],'first_sLat':[TempLat],'first_sLon':[TempLong]})    
        FirstStopLatLong_RawNav =pd.concat([FirstStopLatLong_RawNav,TempDa2])
         
    for idx, row in LastRawNavRow.iterrows():
        RawNavLat1,RawNavLong1 = row[['Lat','Long']]
        GTFS_lastStop.loc[:,'Dist_from_last_RawNav_Pt'] = GTFS_lastStop.apply(lambda row:wr.GetDistanceLatLong_ft(row['last_sLat'],row['last_sLon'],RawNavLat1,RawNavLong1) ,axis=1)
        MaskClosestStop = GTFS_lastStop.Dist_from_last_RawNav_Pt== min(GTFS_lastStop.Dist_from_last_RawNav_Pt )
        TempLat = GTFS_lastStop[MaskClosestStop][['last_sLat','last_sLon']].values[0][0]
        TempLong = GTFS_lastStop[MaskClosestStop][['last_sLat','last_sLon']].values[0][1]
        TempDa2 = pd.DataFrame({'IndexTripTags':[row['IndexTripTags']],'last_sLat':[TempLat],'last_sLon':[TempLong]})    
        LastStopLatLong_RawNav =pd.concat([LastStopLatLong_RawNav,TempDa2])
    
    TestData2 = TestData2.merge(FirstStopLatLong_RawNav,on='IndexTripTags',how='left')
    TestData2.loc[:,'Dist_from_GTFS1stStop'] = TestData2.apply(lambda row:wr.GetDistanceLatLong_ft(row['first_sLat'],row['first_sLon'],row['Lat'],row['Long']) ,axis=1)
    TestData2 = TestData2.merge(LastStopLatLong_RawNav,on='IndexTripTags',how='left')
    TestData2.loc[:,'Dist_from_GTFSlastStop'] = TestData2.apply(lambda row:wr.GetDistanceLatLong_ft(row['last_sLat'],row['last_sLon'],row['Lat'],row['Long']) ,axis=1)
    MinDat = TestData2.groupby(['IndexTripTags'])['Dist_from_GTFS1stStop','Dist_from_GTFSlastStop'].idxmin().reset_index()
    MinDat.rename(columns = {'Dist_from_GTFS1stStop':'LowerBound','Dist_from_GTFSlastStop':"UpperBound"},inplace=True)
    MinDat.loc[:,'LowerBound'] = TestData2.loc[MinDat.loc[:,'LowerBound'],'IndexLoc'].values
    MinDat.loc[:,'UpperBound'] = TestData2.loc[MinDat.loc[:,'UpperBound'],'IndexLoc'].values

    TestData2 = TestData2.merge(MinDat,on='IndexTripTags',how='left')
    MaskGTFS_Trimming = (TestData2.IndexLoc>=TestData2.LowerBound) & (TestData2.IndexLoc<=TestData2.UpperBound)
    TestData2 = TestData2[MaskGTFS_Trimming]
    TestData2 = TestData2[['Lat','Long','Heading','OdomtFt','SecPastSt',
                           'IndexTripTags','Tag','Dist_from_GTFS1stStop','Dist_from_GTFSlastStop']]
    
    Map1 = lambda x: max(x)-min(x)
    SumDat1 =TestData2.groupby('IndexTripTags').agg({'OdomtFt':['min','max',Map1],
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
    SumDat1 = SumDat1.merge(TripSumData,on='IndexTripTags',how='left')
    SumDat1.loc[:,'FileNm'] = key
    ProcessedDataDict[rte_id][key] =  SumDat1
    
FinSumDat = pd.concat(ProcessedDataDict[rte_id].values())
FinSumDat =FinSumDat.merge(TripInventory, on =['FileNm','TripStartTime'],how='left')
FinSumDat.to_excel(os.path.join(path_processed_data,'Route79_TrimSum.xlsx'))

    





