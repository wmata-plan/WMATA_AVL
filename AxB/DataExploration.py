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
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import os 
import sys
import datetime as dt
sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL\AxB")
sys.path.append(r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL")
from MapBox_Token import retMapBoxToken
import folium
from folium.plugins import MarkerCluster
from folium import plugins
# User Defined Functions
from CommonFunctions_AVL import is_numeric
from CommonFunctions_AVL import FindFirstTagLine
from CommonFunctions_AVL import RemoveCAL_APC_Tags
from CommonFunctions_AVL import GetTagInfo
from CommonFunctions_AVL import AddTripStartEndTags
from CommonFunctions_AVL import TripSummaryStartEnd
from CommonFunctions_AVL import PlotTripStart_End



os.chdir(r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL\AxB')
Debug = True
    
# 1 Read Data
#****************************************************************************************************************
RawDataDict = {}
FirstTagDict = {}
ColumnNmMap = {0:'Lat',1:'Long',2:'Heading',3:'DoorState',4:'VehState',5:'OdomtFt',6:'SecPastSt',7:'SatCnt',
                   8:'StopWindow',9:'Blank',10:'LatRaw',11:'LongRaw'}
for file in os.listdir('./RawData'):
    file1 = os.path.join('./RawData',file)
    pat  = re.compile('rawnav(.*).txt') 
    FileNm = pat.search(file).group(1)
    print(FindFirstTagLine(file1))
    FistTagLnNum, FirstTagLine, StartTimeLn = FindFirstTagLine(file1)
    RawDataDict[FileNm] = pd.read_csv(file1,skiprows = FistTagLnNum, header =None)
    FirstTagDict[FileNm] = {'FistTagLnNum':FistTagLnNum,'FirstTagLine':FirstTagLine,'StartTimeLn':StartTimeLn}
    
#2 Experiment with individual data
#****************************************************************************************************************
RawDataDict.keys()
key = '06464190501'
TestData = RawDataDict[key]
BusID = int(key[1:5])
FirstTag = FirstTagDict[key]['FirstTagLine']
FirstTag = [0] + FirstTag

SummaryDataDict={}
ProcessedRawDataDict = {}
RemovedDataCol3_dict = {}
for key,TestData in RawDataDict.items():
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
    TagsData = TestData[TestData.loc[:,6].isna()]
    TripTags,EndOfRoute1 = GetTagInfo(TagsData,FirstTag)
    #Remove rows with tags and rows that have no value in the 3rd column
    # Might need to look back at the 3rd column
    RemoveRows = np.append(EndOfRoute1.IndexTripEnd.values, TripTags.IndexTripTags.values)
    RemoveRows = np.setdiff1d(RemoveRows,np.array([0])) #1st row should not be deleted
    TestData = TestData[~TestData.IndexLoc.isin(RemoveRows)]
    RemovedDataCol3_dict[key] = TestData[TestData.loc[:,3].isna()];TestData= TestData[~TestData.loc[:,3].isna()]
    
    #check if 1st and 2nd column only has lat long 
    try:
        TestData.loc[:,[0,1]] = TestData.loc[:,[0,1]].applymap(lambda x: float(x))#It would not work we All Tags are not removed from the data
    except(ValueError): print('All Tags are not removed from the data')
    #3.3 Get Trip Summary 
    #****************************************************************************************************************
    Data = TestData if Debug else ""
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
    SummaryDataDict[key]=TripSumData1
    ProcessedRawDataDict[key] = TestData1





#Plot the Start and End Points
mapboxAccessToken = retMapBoxToken()
mapboxTilesetId = 'mapbox.satellite'
this_map = folium.Map(zoom_start=16,
    tiles='Stamen Terrain')
folium.TileLayer(tiles='https://api.tiles.mapbox.com/v4/' + mapboxTilesetId + '/{z}/{x}/{y}.png?access_token=' + mapboxAccessToken,
    attr='mapbox.com',name="Mapbox").add_to(this_map)
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
this_map.save("./ProcessedData/TripSummary.html")



# Write Summary to File
OutFi = "./ProcessedData/TripSummaries.xlsx"
writer = pd.ExcelWriter(OutFi,
                        engine='xlsxwriter',
                        datetime_format='mmm d yyyy hh:mm:ss',
                        date_format='mmmm dd yyyy')
for key,value in SummaryDataDict.items():
    value1 = value.copy()
    value1[['TripDurationFromTags','TripDurationFromRawData']] = \
    value1[['TripDurationFromTags','TripDurationFromRawData']].applymap(lambda x: x.total_seconds())
    value1.to_excel(writer,key,index=False)
writer.save()
        

















