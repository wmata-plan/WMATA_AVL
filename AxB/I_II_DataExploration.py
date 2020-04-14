# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 10:07:34 2020
Purpose: Initial data exploration
@author: abibeka
"""


# 0 Hosekeeping and Load Libraries

#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
ipython = get_ipython()

#0.1 Load Libraries
import re
import glob
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import os 
import sys
import datetime as dt
from geopy.distance import geodesic
import zipfile
import shutil
import folium
from folium.plugins import MarkerCluster
from folium import plugins

if os.getlogin() == "WylieTimmerman":
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL\AxB"
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Queue Jump Analysis"
    path_source_data = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\102019 sample")
    path_processed_data = os.path.join(path_sp,r"Client Shared Folder\data\02-processed")
    os.chdir(os.path.join(path_working))
else:
    breakpoint() #I broke your paths, sorry! you'll want to set at least path_processed_data,
    #and set your working directory to wherever AxB is for you
    sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL\AxB")
    sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL")
    os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data')

# User Defined Functions
from I_III_CommonFunctions_DataExploration import is_numeric
from I_III_CommonFunctions_DataExploration import FindFirstTagLine
from I_III_CommonFunctions_DataExploration import RemoveCAL_APC_Tags
from I_III_CommonFunctions_DataExploration import GetTagInfo
from I_III_CommonFunctions_DataExploration import AddTripStartEndTags
from I_III_CommonFunctions_DataExploration import TripSummaryStartEnd
from I_III_CommonFunctions_DataExploration import PlotTripStart_End
from I_III_CommonFunctions_DataExploration import FindFirstTagLine_ZipFile
from I_III_CommonFunctions_DataExploration import CheckValidDataEntry
from I_III_CommonFunctions_DataExploration import GetDistanceforTripSummaryDat

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

# if not sys.warnoptions:
#     import os, warnings
#     warnings.simplefilter("default") # Change the filter in this process
#     os.environ["PYTHONWARNINGS"] = "default" # Also affect subprocesses

Debug = True
# zf = zipfile.ZipFile('./rawnav00001191015.txt.zip') 
# df = pd.read_csv(zf.open('rawnav00001191015.txt'),skiprows=5)
ZipFiles = glob.glob(os.path.join(path_source_data,'*.zip'))
#WT: paths for zipfiles are fun: https://bugs.python.org/issue26283
ZipFileDict = dict((x.replace('\\', '/'),os.path.basename(x.split('.zip')[0])) for x in ZipFiles)

if len(ZipFiles) == 0:
    raise Exception ("No Zip Files found at {}".format(path_source_data))
# 1 Read Data
#****************************************************************************************************************
RawDataDict = {}
FirstTagDict = {}
RawDataDict_WrongBusID = {}
FirstTagDict_WrongBusID  = {}
NoDataDict = {}
NoData_da= pd.DataFrame()
WrongBusID_da = pd.DataFrame()
ColumnNmMap = {0:'Lat',1:'Long',2:'Heading',3:'DoorState',4:'VehState',5:'OdomtFt',6:'SecPastSt',7:'SatCnt',
                   8:'StopWindow',9:'Blank',10:'LatRaw',11:'LongRaw'}

for ZipFolder, ZipFile1 in ZipFileDict.items():
    pat  = re.compile('rawnav(.*).txt') 
    FileNm = pat.search(ZipFile1).group(1)
    #print(FindFirstTagLine_ZipFile(ZipFolder, ZipFile1))
    FistTagLnNum, FirstTagLine, StartTimeLn,HasData,HasCorrectBusID = FindFirstTagLine_ZipFile(ZipFolder, ZipFile1)
    zf = zipfile.ZipFile(ZipFolder)
    if HasData:
        if(HasCorrectBusID):
            RawDataDict[FileNm] = pd.read_csv(zf.open(ZipFile1),skiprows = FistTagLnNum, header =None)
            FirstTagDict[FileNm] = {'FistTagLnNum':FistTagLnNum,'FirstTagLine':FirstTagLine,'StartTimeLn':StartTimeLn}
        else:
            RawDataDict_WrongBusID[FileNm] = pd.read_csv(zf.open(ZipFile1),skiprows = FistTagLnNum, header =None)
            FirstTagDict_WrongBusID[FileNm] = {'FistTagLnNum':FistTagLnNum,'FirstTagLine':FirstTagLine,'StartTimeLn':StartTimeLn}
            tempDa1 = pd.DataFrame(columns=['FileNm','FirstTagLineNo','FirstTagLine'])
            tempDa1.loc[0,['FileNm','FirstTagLineNo','FirstTagLine']] = [FileNm,FistTagLnNum,StartTimeLn]
            NoData_da = pd.concat([NoData_da,tempDa1])
        
    else:
        NoDataDict[FileNm] = {'EndLineNo':FistTagLnNum,'EndLine':StartTimeLn}
        tempDa = pd.DataFrame(columns=['FileNm','EndLineNo','EndLine'])
        tempDa.loc[0,['FileNm','EndLineNo','EndLine']] = [FileNm,FistTagLnNum,StartTimeLn]
        NoData_da = pd.concat([NoData_da,tempDa])
   
# Copy empty files to another directory for checking.
try:
    if not os.path.exists(os.path.join(path_processed_data,'Veh0_2999_NoData')):
        os.makedirs(os.path.join(path_processed_data,'Veh0_2999_NoData'))
except:
    print('Error Dir creation')
for key in NoDataDict.keys():
    shutil.copy(os.path.join(path_source_data,'rawnav'+key+'.txt.zip'),\
                os.path.join(path_processed_data,'Veh0_2999_NoData'))
    
    
# os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data')
# for file in os.listdir('./RawData'):
#     file1 = os.path.join('./RawData',file)
#     pat  = re.compile('rawnav(.*).txt') 
#     FileNm = pat.search(file).group(1)
#     print(FindFirstTagLine(file1))
#     FistTagLnNum, FirstTagLine, StartTimeLn = FindFirstTagLine(file1)
#     RawDataDict[FileNm] = pd.read_csv(file1,skiprows = FistTagLnNum, header =None)
#     FirstTagDict[FileNm] = {'FistTagLnNum':FistTagLnNum,'FirstTagLine':FirstTagLine,'StartTimeLn':StartTimeLn}
# RawDataDict.keys()
# key = '06464190501'
# TestData = RawDataDict[key]
# BusID = int(key[1:5])
# FirstTag = FirstTagDict[key]['FirstTagLine']
# FirstTag = [0] + FirstTag 
#2 Experiment with individual data
#****************************************************************************************************************
SummaryDataDict={}
# ProcessedRawDataDict = {}
RemovedData_dict = {}
#TestData = RawDataDict[key]

for key,TestData in RawDataDict.items():
    if key in SummaryDataDict.keys():
        continue
    FirstTag = FirstTagDict[key]['FirstTagLine']
    FirstTag = [0] + FirstTag
    #3 Data Cleaning
    #****************************************************************************************************************
    
    #3.1 Remove "APC" and "CAL" Labels
    #****************************************************************************************************************
    Data = TestData if Debug else ""
    TestData = RemoveCAL_APC_Tags(TestData)
    #3.2 Get the Rows with Tags
    #****************************************************************************************************************
    TestData.reset_index(inplace=True); TestData.rename(columns = {"index":"IndexLoc"},inplace=True)
#    TagsData = TestData[TestData.loc[:,6].isna()]
    TagsData = TestData[~TestData.apply(CheckValidDataEntry,axis=1)]
    TripTags,EndOfRoute1 = GetTagInfo(TagsData,FirstTag)
    #Remove rows with tags and rows that have no value in the 3rd column
    # Might need to look back at the 3rd column
    RemoveRows = np.append(EndOfRoute1.IndexTripEnd.values, TripTags.IndexTripTags.values)
    RemoveRows = np.setdiff1d(RemoveRows,np.array([0])) #1st row should not be deleted. 
    #1st tag would at position 0 but it doesn't affect the data.
    TestData = TestData[~TestData.IndexLoc.isin(RemoveRows)]
    RemovedData_dict[key] = TestData[~TestData.apply(CheckValidDataEntry,axis=1)];TestData=  TestData[TestData.apply(CheckValidDataEntry,axis=1)]
    if(RemovedData_dict[key].shape[0]!=0):
        RemovedData_dict[key].loc[:,"FileNm"] = key
    else:
        RemovedData_dict[key][0] = ''
        RemovedData_dict[key].loc[0,"FileNm"] = key
    #check if 1st and 2nd column only has lat long 
    try:
        TestData.loc[:,[0,1]] = TestData.loc[:,[0,1]].applymap(lambda x: float(x))#It would not work we All Tags are not removed from the data
    except(ValueError): print('All Tags are not removed from the data')
    #3.3 Get Trip Summary 
    #****************************************************************************************************************
    Data = TestData if Debug else ""
    # Add start and end info to the data. Get Trip start and end data from tags and raw data. 
    TestData1, TripSumData,EndTimeFeetDat = AddTripStartEndTags(TestData,TripTags, EndOfRoute1)
    TripSumData,TripEndFtDat = TripSummaryStartEnd(TripSumData,EndTimeFeetDat,ColumnNmMap)
    TripSumData.rename(columns = {'OdomtFt':"StartFt"},inplace=True)
    TripSumData.loc[:,"TripDurationFromTags"] = pd.to_timedelta(TripSumData.loc[:,"EndDateTime"]- TripSumData.loc[:,"StartDateTime"])
    TripSumData.loc[:,"TripDurationFromRawData"] = pd.to_timedelta(TripSumData.EndTm ,unit='s')
    TripSumData.loc[:,"DistanceMi"] =  (TripSumData.EndFt - TripSumData.StartFt)/5280
    TripSumData.loc[:,"TripSpeed_Tags"] = 3600 *TripSumData.DistanceMi/ TripSumData.TripDurationFromTags.dt.total_seconds()
    TripSumData.loc[:,"TripSpeed_RawData"] = 3600* TripSumData.DistanceMi/ TripSumData.TripDurationFromRawData.dt.total_seconds()
    TripSumData1 = TripSumData[['Tag','BusID','Date','TripStartTime','TripEndTime', \
                               'TripDurationFromTags','TripDurationFromRawData',   \
                               'DistanceMi','TripSpeed_Tags','TripSpeed_RawData',  \
                               'SecPastSt','StartFt','StartLat','StartLong','EndLat','EndLong']]
    #3.4 Work with Raw Data    
    #****************************************************************************************************************
    #Divde the Raw data by Trip End time tags 
    TestData1.rename(columns=ColumnNmMap,inplace=True)
    TestData1.drop(columns =['TripStartTime','TripEndTime'],inplace=True) #Get these columns from TripEndFtDat data
    TestData1 = TestData1.merge(TripEndFtDat, on= 'IndexTripTags',how='left')
    TestData1.loc[:,'TripActive'] = TestData1.OdomtFt <= TestData1.EndFt
    #Match the Index in TripTags, EndOfRoute1 with the closest one in TestData
    TripSumData1.loc[:,'FileNm'] = key 
    TestData1.loc[:,'FileNm'] = key
    TripSumData1.loc[:,'Dist_from_LatLong'] = TripSumData1[['StartLat', 'StartLong', 'EndLat', 'EndLong']].apply(GetDistanceforTripSummaryDat, axis=1)
    SummaryDataDict[key]=TripSumData1
   #ProcessedRawDataDict[key] = TestData1 # Do not store this data. Save as independent files!


#4 Write Summary to File
#****************************************************************************************************************
os.getcwd()
OutFi = os.path.join(path_processed_data,"TripSummaries_Veh0_2999.xlsx")
OutFi2 = os.path.join(path_processed_data,"OctProcessedDataVeh0_2999.xlsx")

writer = pd.ExcelWriter(OutFi,
                        engine='xlsxwriter',
                        datetime_format='mmm d yyyy hh:mm:ss',
                        date_format='mmmm dd yyyy')
writer2 = pd.ExcelWriter(OutFi2,
                        engine='xlsxwriter',
                        datetime_format='mmm d yyyy hh:mm:ss',
                        date_format='mmmm dd yyyy')
FinDat = pd.DataFrame()
FinRemoveData = pd.DataFrame()
FinProcessedData = pd.DataFrame()
for key,value in SummaryDataDict.items():
    value1 = value.copy()
    value1.loc[:,'FileNm'] = key
    value1[['TripDurationFromTags','TripDurationFromRawData']] = \
    value1[['TripDurationFromTags','TripDurationFromRawData']].applymap(lambda x: x.total_seconds())
    FinDat = pd.concat([FinDat,value1])
    FinRemoveData = pd.concat([FinRemoveData,RemovedData_dict[key]])
FinDat2 = FinDat.copy()
FinDat2.set_index(['FileNm','TripStartTime'],inplace=True)
FinRemoveData.set_index('FileNm',inplace=True)
#WT: if no records without data or wrong bus ID present, these two lines will error
if len(NoData_da) > 0:
    NoData_da.set_index('FileNm',inplace=True)
if len(WrongBusID_da) > 0:
    WrongBusID_da.set_index('FileNm',inplace=True)

FinDat.to_excel(writer,"SummaryData",index=False)
FinDat2.to_excel(writer,"DebugSummaryData",index=True)
FinRemoveData.to_excel(writer,"RemovedRows",index=True)
NoData_da.to_excel(writer,"NoDataFiles",index=True)
WrongBusID_da.to_excel(writer,"IncorrectTagFiles",index=True)
writer.save()
        
#5 Plot the Start and End Points
#****************************************************************************************************************
this_map = folium.Map(zoom_start=16,
    tiles='Stamen Terrain')
folium.TileLayer('openstreetmap').add_to(this_map)
folium.TileLayer('cartodbpositron').add_to(this_map)
folium.TileLayer('cartodbdark_matter').add_to(this_map) 

for key,value in SummaryDataDict.items():
    value[['DistanceMi','TripSpeed_Tags','TripSpeed_RawData']] = value[['DistanceMi','TripSpeed_Tags','TripSpeed_RawData']].applymap(lambda x: round(x,2))
    fg = folium.FeatureGroup(name=key)
    this_map.add_child(fg)
    StartGrp = plugins.FeatureGroupSubGroup(fg,f"{key} TripStart")
    this_map.add_child(StartGrp)
    EndGrp = plugins.FeatureGroupSubGroup(fg, f"{key} TripEnd")
    this_map.add_child(EndGrp)
    PlotTripStart_End(value,StartGrp,EndGrp)

SumDat = pd.concat(SummaryDataDict.values())
LatLongs = [[x,y] for x,y in zip(SumDat.StartLat,SumDat.StartLong)]
this_map.fit_bounds(LatLongs)
folium.LayerControl(collapsed=False).add_to(this_map)
this_map.save(os.path.join(path_processed_data,"TripSummary.html"))

