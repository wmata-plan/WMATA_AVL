# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 10:09:45 2020
Functions used in the AVL data analysis
@author: abibeka
"""

import zipfile,re,linecache,numpy as np, pandas as pd, datetime as dt\
    ,folium, io, os, shutil, glob
from itertools import islice
#from folium.plugins import MarkerCluster
from geopy.distance import geodesic
from zipfile import BadZipfile

#Parent Functions
#################################################################################################################
    
def GetZippedFilesFromZipDir(ZipDirList,ZippedFilesDirParent):
    '''
    Get the list of files to read from Zipped folder. Also Unzip the parent folder.
    Will Unzip only once. 
    Parameters
    ----------
    ZipDirList : List or str, 
        List of zipped directories with Rawnav data
        or a single zipped directory with Rawnav data.
    Raises
    ------
    IOError
        Input was not a string or a list. This function doesn't
        handle tuples.
    Returns
    -------
    List of Trip files (zipped) that need to be read.

    '''
    if isinstance(ZipDirList, list): 
        'do nothing'
    elif isinstance(ZipDirList,str):
        ZipDirList = [ZipDirList]
    else:
        raise IOError("ZipDirList should be a string or a List of directory")
    FileUniverse = []
    for ZipDir in ZipDirList: 
        if not os.path.exists(ZipDir.split('.zip')[0]):
            with zipfile.ZipFile(ZipDir,'r') as zip:
                zip.extractall(ZippedFilesDirParent)
        ZipDir1 = ZipDir.split('.zip')[0] #Will work even for Unzipped folders
        listFiles = glob.glob(os.path.join(ZipDir1,"*.zip"))
        FileUniverse.extend(listFiles)
    return(FileUniverse)
           
def find_rawnav_routes(FileUniverse, nmax = None, quiet = True): 
    '''   

    Parameters
    ----------
    FileUniverse : str
        Path to zipped folder with rawnav text file. 
        i.e., rawnav02164191003.txt.zip
        Assumes that included text file has the same name as the zipped file,
        minus the '.zip' extension.
        Note: For absolute paths, use forward slashes.
    nmax : int, optional
        limit files to read to this number. If None, all zip files read.
    quiet : boolean, optional
        Whether to print status. The default is True.

    Returns
    -------
    ReturnDict.

    '''
    FileUniverseSet = FileUniverse[0:nmax]

    # Setup dataframe for iteration
    FileUniverseDF = pd.DataFrame({'fullpath' : FileUniverseSet})
    FileUniverseDF['filename'] = FileUniverseDF['fullpath'].str.extract('(rawnav\d+.txt)')
    FileUniverseDF['file_busid'] = FileUniverseDF['fullpath'].str.extract('rawnav(\d{5})\S+.txt')
    FileUniverseDF['file_id'] = FileUniverseDF['fullpath'].str.extract('rawnav(\d+).txt')
    FileUniverseDF['file_busid'] = pd.to_numeric(FileUniverseDF['file_busid'])
    
    # Get Tags and Reformat
    FileUniverseDF['taglist'] = [FindAllTags(path, quiet = quiet) for path in FileUniverseDF['fullpath']]
    FileUniverseDF = FileUniverseDF.explode('taglist')
    
    FileUniverseDF[['line_num','route_pattern','tag_busid','tag_date','tag_time','Unk1','CanBeMiFt']] = FileUniverseDF['taglist'].str.split(',', expand = True)
    FileUniverseDF[['route','pattern']] = FileUniverseDF['route_pattern'].str.extract('^(?:\s*)(?:(?!PO))(?:(?!PI))(?:(?!DH))(\S+)(\d{2})$')
    
    # Convert Column Types and Create new ones
    # TODO: add more as necessary for datetime, hour, time period, etc.
    # Changing line_nums type created problems, so leaving as is.
    # FileUniverseDF['line_num'] = pd.to_numeric(FileUniverseDF['line_num'])
    FileUniverseDF['tag_busid'] = pd.to_numeric(FileUniverseDF['tag_busid'])
    FileUniverseDF['tag_date'] = pd.to_datetime(FileUniverseDF['tag_date'], infer_datetime_format=True)
    FileUniverseDF['wday'] = FileUniverseDF['tag_date'].dt.day_name()
    
    return(FileUniverseDF)
#Nested Functions
#################################################################################################################


def MoveEmptyIncorrectLabelFiles(File, path_source_data, Issue='EmptyFiles'):
    '''
    

    Parameters
    ----------
    File : str
        Rawnav files with empty or incorrect BusID
    path_source_data : str
        Sending the file "File" to a directory in path_source_data.
    Issue : str, optional
        Type of issue with the file: missing/Empty. The default is 'EmptyFiles'.

    Returns
    -------
    None.

    '''
    # Copy empty files to another directory for checking.
    pat  = re.compile('.*(Vehicles\s*[0-9]*-[0-9]*)') 
    VehNos =pat.search(File).group(1)
    MoveFolderName = "EmptyMissClassFiles//" + VehNos
    MoveDir = os.path.join(path_source_data,MoveFolderName,Issue)
    pat  = re.compile('(rawnav.*.txt)') 
    try:
        if not os.path.exists(MoveDir):
            os.makedirs(MoveDir)
    except:
        print('Error Dir creation')
    shutil.copy(File,MoveDir)  #Will change it to "move" later
    return(None)



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


def FindFirstTagLine_ZipFile(ZipFolder, ZipFile1):
    '''
    Parameters
    ----------
    ZipFolder: str
        Zipped folder with the text file. For absolute paths, use forward slashes.
        i.e., rawnav02164191003.txt.zip
    ZipFile1 : str
        Read the 1st 100 lines of this file to find when the csv format starts, 
        e.g.rawnav02164191003.txt

    Returns
    -------
    FirstTagLineNum: int
        Line number with the 1st useful info 
    FirstTagLineElements: list
        First Line with Bus ID, Time etc..
    StartTimeLine: str
        Also contains the start time
    HasData: Bool
        Boolean to check if file has data
    HasCorrectBusID: Bool
        Boolean to check if the Bus ID is correct in the 1st line
    '''
    zf = zipfile.ZipFile(ZipFolder)
    # Get BusID
    pat  = re.compile('rawnav(.*).txt') 
    BusID = pat.search(ZipFile1).group(1)[1:5]
    BusID = int(BusID)
    number_of_lines = 100 # # lines to search 
    dateFormat = re.compile('^\d{1,2}\/\d{1,2}\/\d{2}$') 
    timeFormat = re.compile('^\d{1,2}:\d{1,2}:\d{2}$')
    FirstTagLineNum = 1
    FirstTagLineElements = []
    StartTimeLine = ""
    HasData= False
    HasCorrectBusID = False
    with io.TextIOWrapper(zf.open(ZipFile1, 'r'),encoding="utf-8") as input_file:
        lines_cache = islice(input_file, number_of_lines)
        for current_line in lines_cache:
            StartTimeLine = current_line
            tempList = current_line.split(',')
            # print(tempList)
            #Check for this pattern ['PO03408', '6431', '04/30/19', '07:12:00', '45145', '05280\n']
            if(len(tempList)>=4): 
                if(
                 bool(re.match(dateFormat,tempList[2]))&
                 bool(re.match(timeFormat,tempList[3]))
                  ): 
                    if(int(tempList[1])==BusID):
                        StartTimeLine = current_line
                        FirstTagLineElements = tempList
                        FirstTagLineElements = [x.strip() for x in FirstTagLineElements]
                        HasData = True
                        HasCorrectBusID = True
                        break       
                    else:
                        StartTimeLine = current_line
                        FirstTagLineElements = tempList
                        FirstTagLineElements = [x.strip() for x in FirstTagLineElements]
                        HasData = True   
                        HasCorrectBusID = False
                        break
            FirstTagLineNum = FirstTagLineNum+1
    return([FirstTagLineNum,FirstTagLineElements,StartTimeLine,HasData,HasCorrectBusID])


def CheckValidDataEntry(row):
    '''
    row: Pandas DataFrame Row
    Check if a row is valid i.e. has
    lat, long, heading, DoorState, VehinMotion, Odometer and TimeSec data.
    Also validate the data range.
    '''
    IsValidEntry = False
    try:
        Lat = float(row[0])
        Long = float(row[1])
        Heading = float(row[2])
        DoorState = row[3]
        VehinMotion = row[4]
        OdoMet = int(row[5])
        TimeSec = int(row[6])
        if((-90<=Lat<=90)&(-180<=Long<=180)&(0<=Heading<=360)&
           (DoorState in ['O','C'])& (VehinMotion in['M','S'])):
            IsValidEntry = True
    except: ""
    return(IsValidEntry)

def RemoveCAL_APC_Tags(Data):
    #Remove all rows with "CAL" label
    MaskCal = Data.iloc[:,0].str.upper().str.strip() =="CAL"
    Data = Data[~MaskCal]
    #print(f"Removed {sum(MaskCal)} rows for CAL")
    #Reset Index for reassigning tags based on "ApC"
    Data.reset_index(drop="True",inplace=True)     
    #Add Stop Tag Based on "APC"
    MaskAPC = Data.iloc[:,0].str.upper().str.strip() =="APC"
    APCTags = np.array(Data[MaskAPC].index)
    APCTag_Minus1 = APCTags-1 # Get the row previous to the APC tag
    #Check if some of the APCTag_Minus1 rows have invalid entries
    #Replace these invalid entries with valid GPS entries
    if sum(~Data.loc[APCTag_Minus1].apply(CheckValidDataEntry,axis=1))>0:
        mask = ~Data.loc[APCTag_Minus1].apply(CheckValidDataEntry,axis=1)
        #Index that need replacement
        ReplaceIndices= Data.loc[APCTag_Minus1].loc[mask,:].index
        ValidIndicesDict = {}
        for ReplaceIndex in ReplaceIndices:
            while(not CheckValidDataEntry(Data.loc[ReplaceIndex,:])):
                ValidIndicesDict[ReplaceIndex] = ReplaceIndex-1
                ReplaceIndex = ReplaceIndex-1
        APCTag_Minus1 = [ValidIndicesDict[x] if x in ValidIndicesDict.keys() else x for x in APCTag_Minus1]    
   #APC tag is not always preceeded by "O". Ignore for now
    #Is the row above "APC" all open and stopped
    # CheckAPC = Data.loc[APCTag_Minus1].groupby([3,4])[3].count()
    # CheckCounts = 0
    # for indexVal in CheckAPC.index.values:
    #     if indexVal in [('O', 'M'), ('O', 'S'), ('C',"S")]:
    #         CheckCounts = CheckCounts + CheckAPC[indexVal]
    #     print(f"Vehicle State Before APC Tag: {indexVal}")
    # assert(CheckCounts== sum(MaskAPC)) # An "APC" should mean that the bus was stopped
    Data.loc[APCTag_Minus1,"RowBeforeAPC"] = False
    Data.loc[APCTag_Minus1,"RowBeforeAPC"] = True
    Data = Data[~MaskAPC]
    #print(f"Removed {sum(MaskAPC)} rows for APC counter")
    return(Data)



def GetTagInfo(TagsData,FirstTag):
    '''
    Search for 'Buswares navigation reported end of route' Tags and
    Route info tags like "PO", "PI", "DH" and "7901"...
    '''
    BusID = FirstTag[2] #3rd element in FirstTag list
    if TagsData.shape[0]> 0 :
        TagsData.loc[:,0] = TagsData.loc[:,0].str.strip()
        BusEndNav =TagsData.loc[:,0].str.find('Buswares navigation reported end of route')
        BusEndNav2 =TagsData.loc[:,0].str.find('Buswares is now using route zero')
        MaskEndRoute = (BusEndNav!=-1)|(BusEndNav2!=-1)
        TagsData.loc[:,"HasRouteEndTag"] = False
        TagsData.loc[MaskEndRoute,"HasRouteEndTag"] =  True
        #---------------------
        #Analyze Tags with "Busware ...." 
        EndOfRoute  = TagsData[TagsData.HasRouteEndTag]
        #Pat = re.compile('/(.*)Buswares navigation reported end of route') 
        Pat = re.compile('/(.*)((Buswares navigation reported end of route)|(Buswares is now using route zero))') 
        EndOfRoute.loc[:,0] = EndOfRoute[0].apply(lambda x: Pat.search(x).group(1).strip())
        EndOfRoute.rename(columns={'IndexLoc':'IndexTripEnd',0:"TripEndTime"},inplace=True); EndOfRoute= EndOfRoute[['IndexTripEnd','TripEndTime']]
        #---------------------        
        #Look at tags which do not have "Busware navi...." line"    
        TripTags = TagsData[~TagsData.HasRouteEndTag]
        # Assign Nan with -99 Tag and convert the column with BusID to int
        TripTags.loc[:,1] = TripTags.loc[:,1].apply(lambda x: int(x) if x==x else -99)
        TripTags = TripTags[TripTags.loc[:,1] == int(BusID)] #Remove Garbage lines: "Busware shutting down...."
        TripTags.rename(columns = {0:"Tag",1:"BusID",2:"Date",3:"TripStartTime",4:"Unk1",5:"CanBeMiFt"},inplace=True)
        TripTags = TripTags[['IndexLoc','Tag','BusID','Date','TripStartTime','Unk1','CanBeMiFt']]
    else:
        TripTags = pd.DataFrame(columns=['IndexLoc','Tag','BusID','Date','TripStartTime','Unk1','CanBeMiFt'])
        EndOfRoute = pd.DataFrame(columns=['IndexTripEnd','TripEndTime'])
    #Need to add the info from the 1st tag that was extracted before.
    FirstTag = pd.DataFrame(dict(zip(['IndexLoc','Tag','BusID','Date','TripStartTime','Unk1','CanBeMiFt'],FirstTag)),index=[0])
    TripTags = pd.concat([FirstTag,TripTags])
    TripTags.rename(columns={'IndexLoc':'IndexTripTags'},inplace=True)
    TripTags.IndexTripTags = TripTags.IndexTripTags.astype(int)
    EndOfRoute.loc[:,'IndexTripEnd'] =EndOfRoute.loc[:,'IndexTripEnd'].astype(int)
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
    EndTimeFeetDat: End time and odometer reading based on tags like
    ['PO03408', '6431', '04/30/19', '07:12:00', '45145', '05280\n']; Not using "Busware ...." tags.
    Will be used later to handle missing "Busware ..." tags
    '''
    #Get the closest index in the data above the tag row where the tag info should be added
    # TripTags indicate trip start so use 'forward'
    Data.loc[:,'IndexLoc'] =Data.loc[:,'IndexLoc'].astype(int)
    TripTags = pd.merge_asof(TripTags,Data,left_on="IndexTripTags",right_on="IndexLoc",direction= 'forward')
    TripTags = TripTags[['IndexLoc','IndexTripTags','Tag','BusID','Date','TripStartTime']]
    #------------------------------
    # EndOfRoute1 would use previous row index
    EndOfRoute1 = pd.merge_asof(EndOfRoute1,Data,left_on="IndexTripEnd",right_on="IndexLoc",direction= 'backward')
    EndOfRoute1 =EndOfRoute1[['IndexLoc','IndexTripEnd','TripEndTime']]
    #------------------------------
    Data = Data.merge(TripTags,on = "IndexLoc",how="left")
    #Forward Fill the trip tags/ IndexTripTags to identify Unique trips
    Data.loc[:,['IndexTripTags','Tag']] = Data.loc[:,['IndexTripTags','Tag']].fillna(method='ffill')
    EndTimeFeetDat = Data.groupby(['IndexTripTags'])[[0,1,5,6]].last().reset_index() 
    # Get the last index values for trips 
    #with missing  "Buswares end of route..." notification
    #Trips with  "Buswares end of route..." notification would not need EndTimeFeetDat data 
    EndTimeFeetDat.rename(columns = {0:"EndLat",1:"EndLong",5:"EndFt",6:"EndTm"},inplace=True)
    #------------------------------
    Data = Data.merge(EndOfRoute1,on = "IndexLoc",how='left')
    #------------------------------
    #Find Rows with BusID and TripEnd Times
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
    #Get Rows which have end time; Rows with  "Busware..."
    EndTimeData2 = TripSumData[~TripSumData.IndexTripEnd.isna()]; EndTimeData2.rename(columns={0:"EndLat",1:"EndLong",5:"EndFt",6:"EndTm"},inplace=True)
    EndTimeData2 = EndTimeData2[['IndexTripTags',"EndTm","EndFt",'EndLat','EndLong']].sort_values(['IndexTripTags','EndTm','EndFt'])
    EndTimeData2= EndTimeData2.groupby('IndexTripTags').first().reset_index() # Only use the 1st tag for end times
    # Get rows where "Buswares end of route..." tag is not preesent
    # Need to use the EndTimeFeetDat to substitute end feet and time
    IndexWithNoBusWareTags= np.setdiff1d(EndTimeFeetDat.IndexTripTags, EndTimeData2.IndexTripTags) 
    EndTimeFeetDat = EndTimeFeetDat[EndTimeFeetDat.IndexTripTags.isin(IndexWithNoBusWareTags)]
    EndTimeDataClean = pd.concat([EndTimeData2,EndTimeFeetDat]).reset_index(drop=True)
    #-----------------------------------------------------------------------------------------
    #1st fill the end time within trips based on "Buswares end of route..." notification
    TripSumData.loc[:,'TripEndTime'] = TripSumData.groupby('IndexTripTags')['TripEndTime'].fillna(method='bfill')
    # Now use the start time of next trip for trips which do not have "Buswares end of route..." notification
    TripSumData = TripSumData[~TripSumData.TripStartTime.isna()]
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
    #Last Row sometimes doesn't have end time; need error handling.
    #This issue should not be there with StartDateTime
    def func(x):
        try:
            return dt.datetime.strptime(" ".join(x),"%m/%d/%y %H:%M:%S")
        except:
            return pd.NaT
    TripSumData.loc[:,"EndDateTime"] = TripSumData[['Date','TripEndTime']].apply(func ,axis=1)
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
        
        
#https://github.com/geopy/geopy
# Make this function Generic ---later
def GetDistanceforTripSummaryDat(row):
    StartLat, StartLong, EndLat, EndLong = row['StartLat'], row['StartLong'], row['EndLat'], row['EndLong']
    distance_miles = -999
    distance_miles = geodesic((StartLat, StartLong), (EndLat, EndLong)).miles
    return(distance_miles)

def GetDistanceLatLong_ft(Lat1, Long1, Lat2, Long2):
    distance_ft = geodesic((Lat1, Long1), (Lat2, Long2)).feet
    return(distance_ft)

