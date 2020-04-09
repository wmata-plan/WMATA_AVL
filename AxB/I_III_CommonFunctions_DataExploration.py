# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 10:09:45 2020
Functions used in the AVL data analysis
@author: abibeka
"""

from itertools import islice
import re
import linecache
import numpy as np
import pandas as pd
import datetime as dt
from MapBox_Token import retMapBoxToken
import folium
from folium.plugins import MarkerCluster

def is_numeric(s):
    '''
    Check if Lat/Long is a String. Data has tags like APC : Automatic passenger count
    CAL, Tags about Trip start and End. Find the location of these tags.
    '''
    try:
        float(s)
        return True
    except(ValueError, TypeError):
        return False
    
    
    
def FindFirstTagLine(filename):
    '''
    Parameters
    ----------
    filename : str
        Read the 1st 100 lines to find when the csv format starts.

    Returns
    -------
    FirstTagLineNum: int
        Line number with the 1st useful info 
    FirstTagLineElements: list
        First Line with Bus ID, Time etc..
    StartTimeLine: Also contains the start time
    '''
    # Get BusID
    pat  = re.compile('rawnav(.*).txt') 
    BusID = pat.search(filename).group(1)[1:5]
    number_of_lines = 100 # # lines to search 
    dateFormat = re.compile('^\d{1,2}\/\d{1,2}\/\d{2}$') 
    timeFormat = re.compile('^\d{1,2}:\d{1,2}:\d{2}$')
    FirstTagLineNum = 1
    FirstTagLineElements = []
    with open(filename, 'r') as input_file:
        lines_cache = islice(input_file, number_of_lines)
        for current_line in lines_cache:
            tempList = current_line.split(',')
            #Check for this pattern ['PO03408', '6431', '04/30/19', '07:12:00', '45145', '05280\n']
            if(len(tempList)>=4): 
                if(
                 (tempList[1]==BusID) &
                 bool(re.match(dateFormat,tempList[2]))&
                 bool(re.match(timeFormat,tempList[3]))
                  ): 
                    FirstTagLineElements = tempList
                    break
            FirstTagLineNum = FirstTagLineNum+1
    StartTimeLine = linecache.getline(filename, FirstTagLineNum-1)
    return([FirstTagLineNum,FirstTagLineElements,StartTimeLine])



def RemoveCAL_APC_Tags(Data):
    #Remove all rows with "CAL" label
    MaskCal = Data.iloc[:,0].str.upper().str.strip() =="CAL"
    Data = Data[~MaskCal]
    print(f"Removed {sum(MaskCal)} rows for CAL")
    #Reset Index for reassigning tags based on "ApC"
    Data.reset_index(drop="True",inplace=True) 
    #Add Stop Tag Based on "APC"
    MaskAPC = Data.iloc[:,0].str.upper().str.strip() =="APC"
    APCTags = np.array(Data[MaskAPC].index)
    APCTag_Minus1 = APCTags-1 # Get the row previous to the APC tag
    #Is the row above "APC" all open and stopped
    CheckAPC = Data.loc[APCTag_Minus1].groupby([3,4])[3].count()
    CheckCounts = 0
    for indexVal in CheckAPC.index.values:
        if indexVal in [('C', 'S'), ('O', 'S')]:
            CheckCounts = CheckCounts + CheckAPC[indexVal]
        print(f"Vehicle State Before APC Tag: {indexVal}")
    assert(CheckCounts== sum(MaskAPC)) # An "APC" should mean that the bus was stopped
    Data.loc[APCTag_Minus1,"BsusStopped"] = False
    Data.loc[APCTag_Minus1,"BusStopped"] = True
    Data = Data[~MaskAPC]
    print(f"Removed {sum(MaskAPC)} rows for APC counter")
    return(Data)



def GetTagInfo(TagsData,FirstTag):
    '''
    Search for 'Buswares navigation reported end of route' Tags and
    Route info tags like "PO", "PI", "DH" and "7901"...
    '''
    BusID = FirstTag[2] #3rd element in FirstTag list
    TagsData.loc[:,0] = TagsData.loc[:,0].str.strip()
    BusEndNav =TagsData.loc[:,0].str.find('Buswares navigation reported end of route')
    BusEndNav2 =TagsData.loc[:,0].str.find('Buswares is now using route zero')
    MaskEndRoute = (BusEndNav!=-1)|(BusEndNav2!=-1)
    TagsData.loc[:,"HasRouteEndTag"] = False
    TagsData.loc[MaskEndRoute,"HasRouteEndTag"] =  True
    EndOfRoute  = TagsData[TagsData.HasRouteEndTag]
    #Pat = re.compile('/(.*)Buswares navigation reported end of route') 
    Pat = re.compile('/(.*)((Buswares navigation reported end of route)|(Buswares is now using route zero))') 
    EndOfRoute.loc[:,0] = EndOfRoute[0].apply(lambda x: Pat.search(x).group(1).strip())
    EndOfRoute.rename(columns={'IndexLoc':'IndexTripEnd',0:"TripEndTime"},inplace=True); EndOfRoute= EndOfRoute[['IndexTripEnd','TripEndTime']]
    #---------------------            
    TripTags = TagsData[~TagsData.HasRouteEndTag]
    # Assign Nan with -99 Tag and convert the column with BusID to int
    TripTags.loc[:,1] = TripTags.loc[:,1].apply(lambda x: int(x) if x==x else -99)
    TripTags = TripTags[TripTags.loc[:,1] == int(BusID)]
    TripTags.rename(columns = {0:"Tag",1:"BusID",2:"Date",3:"TripStartTime",4:"Unk1",5:"CanBeMiFt"},inplace=True)
    TripTags = TripTags[['IndexLoc','Tag','BusID','Date','TripStartTime','Unk1','CanBeMiFt']]
    FirstTag = pd.DataFrame(dict(zip(['IndexLoc','Tag','BusID','Date','TripStartTime','Unk1','CanBeMiFt'],FirstTag)),index=[0])
    TripTags = pd.concat([FirstTag,TripTags])
    TripTags.rename(columns={'IndexLoc':'IndexTripTags'},inplace=True)
    return(TripTags,EndOfRoute)



def AddTripStartEndTags(Data,TripTags, EndOfRoute1):
    '''
    Parameters
    ----------
    Data : TYPE
        Data with all columns.
    TripTags : TYPE
        Trip Start tags.
    EndOfRoute1 : TYPE
        Trip End Tags.
    Returns
    -------
    Data: Data with Trip start and End tags
    TripSumData: Just trip Start and End Tags

    '''
    #Get the closest index in the data above the tag row where the tag info should be added
    # TripTags indicate trip start so use 'forward'
    TripTags = pd.merge_asof(TripTags,Data,left_on="IndexTripTags",right_on="IndexLoc",direction= 'forward')
    TripTags = TripTags[['IndexLoc','IndexTripTags','Tag','BusID','Date','TripStartTime']]
    EndOfRoute1 = pd.merge_asof(EndOfRoute1,Data,left_on="IndexTripEnd",right_on="IndexLoc",direction= 'backward')
    EndOfRoute1 =EndOfRoute1[['IndexLoc','IndexTripEnd','TripEndTime']]
    Data = Data.merge(TripTags,on = "IndexLoc",how="left")
    #Forward Fill the trip tags/ IndexTripTags to identify Unique trips
    Data.loc[:,['IndexTripTags','Tag']] = Data.loc[:,['IndexTripTags','Tag']].fillna(method='ffill')
    EndTimeFeetDat = Data.groupby(['IndexTripTags'])[[0,1,5,6]].last().reset_index() # Get thelast index values for trips 
    #with missing  "Buswares end of route..." notification
    EndTimeFeetDat.rename(columns = {0:"EndLat",1:"EndLong",5:"EndFt",6:"EndTm"},inplace=True)
    Data = Data.merge(EndOfRoute1,on = "IndexLoc",how='left')
    MaskTripDetails = (~Data.BusID.isna()) |(~Data.IndexTripEnd.isna())
    TripSumData  = Data[MaskTripDetails]
    return(Data, TripSumData,EndTimeFeetDat)

def TripSummaryStartEnd(TripSumData,EndTimeFeetDat,ColumnNmMap):
    '''
    Parameters
    ----------
    TripSumData : pd.DataFrame
        Summary data obtained from AddTripStartEndTags function
    EndTimeFeetDat : pd.DataFrame
        Trip End time and feet data. Needed for trips with no trip end tag.
    ColumnNmMap : dict
        Dict for renaming columns.
    Returns
    -------
    TripSumData: Trip Summary data. One row per trip
    TripEndFtDat: Same as TripSumData---subset
    '''
    
    EndTimeData2 = TripSumData[~TripSumData.IndexTripEnd.isna()]; EndTimeData2.rename(columns={0:"EndLat",1:"EndLong",5:"EndFt",6:"EndTm"},inplace=True)
    EndTimeData2 = EndTimeData2[['IndexTripTags',"EndTm","EndFt",'EndLat','EndLong']].sort_values(['IndexTripTags','EndTm','EndFt'])
    EndTimeData2= EndTimeData2.groupby('IndexTripTags').first().reset_index() # Only use the 1st tag for end times
    IndexWithNoBusWareTags= np.setdiff1d(EndTimeFeetDat.IndexTripTags, EndTimeData2.IndexTripTags) # Get rows where "Buswares end of route..." tag is not preesent
    EndTimeFeetDat = EndTimeFeetDat[EndTimeFeetDat.IndexTripTags.isin(IndexWithNoBusWareTags)]
    EndTimeDataClean = pd.concat([EndTimeData2,EndTimeFeetDat]).reset_index(drop=True)
    #-----------------------------------------------------------------------------------------
    #1st fill the end time within trips based on "Buswares end of route..." notification
    TripSumData.loc[:,'TripEndTime'] = TripSumData.groupby('IndexTripTags')['TripEndTime'].fillna(method='bfill')
    # Now use the start time of next trip for trips which do not have "Buswares end of route..." notification
    TripSumData = TripSumData[TripSumData.IndexTripEnd.isna()]
    #Get end time by using the start time from the next row
    TripSumData.sort_values('IndexLoc',inplace=True)
    TripSumData.loc[:,'tempTime'] = TripSumData.TripStartTime.shift(-1)
    TripSumData.loc[TripSumData.TripEndTime.isna(),'TripEndTime'] =\
        TripSumData.loc[TripSumData.TripEndTime.isna(),'tempTime']
    TripSumData.rename(columns=ColumnNmMap,inplace=True)
    TripSumData.rename(columns = {"Lat":"StartLat","Long":"StartLong"},inplace=True)
    TripSumData = TripSumData[["Tag",'BusID','Date','TripStartTime','TripEndTime',
                               'IndexLoc','IndexTripTags','OdomtFt','SecPastSt','StartLat','StartLong']]
    TripSumData = TripSumData.merge(EndTimeDataClean,on="IndexTripTags",how='left') #Trip Summary with End times
    TripSumData.loc[:,"StartDateTime"] = TripSumData[['Date','TripStartTime']].apply(lambda x: dt.datetime.strptime(" ".join(x),"%m/%d/%y %H:%M:%S"),axis=1)
    TripSumData.loc[:,"EndDateTime"] = TripSumData[['Date','TripEndTime']].apply(lambda x: dt.datetime.strptime(" ".join(x),"%m/%d/%y %H:%M:%S"),axis=1)
    #Get Trip End point
    TripEndFtDat = TripSumData[['IndexTripTags','EndFt','StartDateTime','EndDateTime']]
    return(TripSumData,TripEndFtDat)



def PlotTripStart_End(SumDat,StartGrp,EndGrp):
    # mc_start = MarkerCluster(name='TripStart').add_to(this_map)
    # mc_end = MarkerCluster(name='TripEnd').add_to(this_map)    
    popup_field_list = list(SumDat.columns)     
    for i,row in SumDat.iterrows():
        label = '<br>'.join([field + ': ' + str(row[field]) for field in popup_field_list])
        folium.Marker(
                location=[row.StartLat, row.StartLong],
                popup=folium.Popup(html = label,parse_html=False,max_width='150'),
                icon=folium.Icon(color='darkblue', icon='ok-sign')).add_to(StartGrp)
        folium.Marker(
            location=[row.EndLat, row.EndLong],
            popup=folium.Popup(html = label,parse_html=False,max_width='150'),
            icon=folium.Icon(color='darkred', icon='ok-sign')).add_to(EndGrp)