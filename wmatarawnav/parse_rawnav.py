# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 10:09:45 2020
Functions used in the AVL data analysis
@author: abibeka
"""

import zipfile,re,linecache,numpy as np, pandas as pd, datetime as dt\
    ,folium, io, os, shutil, glob, logging
import pandasql as ps
from itertools import islice
#from folium.plugins import MarkerCluster
from geopy.distance import geodesic # Can't vectorize
from zipfile import BadZipfile
import geopandas as gpd
from shapely.geometry import Point
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

def load_rawnav_data(ZipFolderPath, skiprows): 
    '''
    Parameters
    ----------
    ZipFolderPath : str
        Path to the zipped rawnav.txt file..
    skiprows : int
        Number of rows with metadata.

    Returns
    -------
    pd.DataFrame with the file info.

    '''
    zf = zipfile.ZipFile(ZipFolderPath)
    # Get Filename
    namepat = re.compile('(rawnav\d+\.txt)') 
    ZipFileName = namepat.search(ZipFolderPath).group(1)     
    RawData = pd.read_csv(zf.open(ZipFileName),skiprows = skiprows, header =None)
    return(RawData)


def clean_rawnav_data(DataDict): 
    # TODO: write documentation
    rawnavdata = DataDict['RawData']
    taglineData = DataDict['tagLineInfo']
    try:
        Temp = taglineData.NewLineNo.values.flatten()
        TagIndices= np.delete(Temp, np.where(Temp==-1))
        CheckTagLineData = rawnavdata.loc[TagIndices,:]
        CheckTagLineData[[1,4,5]] = CheckTagLineData[[1,4,5]].astype(int)
        CheckTagLineData.loc[:,'taglist']=(CheckTagLineData[[0,1,2,3,4,5]].astype(str)+',').sum(axis=1).str.rsplit(",",1,expand=True)[0]
        CheckTagLineData.loc[:,'taglist']= CheckTagLineData.loc[:,'taglist'].str.strip()
        infopat ='^\s*(\S+),(\d{1,5}),(\d{2}\/\d{2}\/\d{2}),(\d{2}:\d{2}:\d{2}),(\S+),(\S+)'
        assert((~CheckTagLineData.taglist.str.match(infopat, re.S)).sum()==0)
    except:
        logging.error("TagLists Did not match")
    #Keep index references. Will use later
    rawnavdata.reset_index(inplace=True); rawnavdata.rename(columns = {"index":"IndexLoc"},inplace=True)
    #Get End of route Info
    taglineData, deleteIndices1 = AddEndRouteInfo(rawnavdata, taglineData)
    rawnavdata = rawnavdata[~rawnavdata.index.isin(np.append(TagIndices,deleteIndices1))]
    #Remove APC and CAL labels and keep APC locations. Can merge_asof later.
    rawnavdata, APCTagLoc = RemoveAPC_CAL_Tags(rawnavdata)
    CheckDat = rawnavdata[~rawnavdata.apply(CheckValidDataEntry,axis=1)]
    Pat = re.compile('^\s*/\s*(?P<TripEndTime>\d{2}:\d{2}:\d{2})\s*(?:Buswares.*SHUTDOWN|bwrawnav)',re.S|re.I) 
    assert(sum(~(CheckDat[0].str.match(Pat)))==0) ,"Did not handle some additional lines in CheckDat"
    rawnavdata = rawnavdata[rawnavdata.apply(CheckValidDataEntry,axis=1)]
    #Add the APC tag to the rawnav data to identify stops
    APClocDat = pd.Series(APCTagLoc,name='APCTagLoc')
    APClocDat = pd.merge_asof(APClocDat,rawnavdata[["IndexLoc"]],left_on = "APCTagLoc",right_on="IndexLoc") # default direction is backward
    rawnavdata.loc[:,'RowBeforeAPC'] = False
    rawnavdata.loc[APClocDat.IndexLoc,'RowBeforeAPC'] = True
    taglineData.rename(columns={'NewLineNo':"IndexTripStart"},inplace=True)
    SummaryData = GetTripSummary(data= rawnavdata,taglineData = taglineData)
    ColumnNmMap = {0:'Lat',1:'Long',2:'Heading',3:'DoorState',4:'VehState',5:'OdomtFt',6:'SecPastSt',7:'SatCnt',
                   8:'StopWindow',9:'Blank',10:'LatRaw',11:'LongRaw'}
    rawnavdata.rename(columns =ColumnNmMap,inplace=True )
    rawnavdata = AddTripDividers(rawnavdata,SummaryData )
    returnDict = {'rawnavdata':rawnavdata,'SummaryData':SummaryData}
    return(returnDict)
     
# def summarize_rawnav_trip(): 
# def import_GTFS_data(): 

#Nested Functions
#################################################################################################################
def AddTripDividers(data, SummaryData):
    SummaryData.columns
    TagsTemp = SummaryData[['route_pattern','route', 'pattern','IndexTripStartInCleanData','IndexTripEndInCleanData']]
    q1  = '''SELECT data.IndexLoc,data.Lat,data.Long,data.Heading,data.DoorState,data.VehState,data.OdomtFt,data.SecPastSt,
        data.SatCnt,data.StopWindow,data.Blank,data.LatRaw,data.LongRaw,data.RowBeforeAPC,
        TagsTemp.route_pattern,TagsTemp.route,TagsTemp.pattern,TagsTemp.IndexTripStartInCleanData,TagsTemp.IndexTripEndInCleanData
        FROM data LEFT JOIN TagsTemp on data.IndexLoc BETWEEN  TagsTemp.IndexTripStartInCleanData and TagsTemp.IndexTripEndInCleanData
    '''
    data = ps.sqldf(q1, locals())
    return(data)    
    
def GetTripSummary(data, taglineData):
    temp = taglineData[['IndexTripStart','IndexTripEnd']]
    temp = temp.astype('int32')
    rawDaCpy = data[['IndexLoc',0,1,5,6]].copy()
    rawDaCpy[['IndexLoc',5,6]]= rawDaCpy[['IndexLoc',5,6]].astype('int32')
    rawDaCpy.rename(columns={0:'Lat',1:'Long',5:'OdomtFt',6:'SecPastSt'},inplace=True)
    temp = pd.merge_asof(temp,rawDaCpy,left_on = "IndexTripStart",right_on ="IndexLoc" , direction='forward')
    temp.rename(columns = {'Lat':"LatStart",'Long':"LongStart",
                           'OdomtFt': "OdomFtStart",'SecPastSt':"SecStart","IndexLoc":"IndexTripStartInCleanData"},inplace=True)
    temp = pd.merge_asof(temp,rawDaCpy,left_on = "IndexTripEnd",right_on ="IndexLoc" , direction='backward')
    temp.rename(columns = {'Lat':"LatEnd",'Long':"LongEnd",'OdomtFt': "OdomFtEnd",'SecPastSt':"SecEnd",
                           "IndexLoc":"IndexTripEndInCleanData"},inplace=True)
    temp.loc[:,"TripDurFromSec"] = temp.SecEnd - temp.SecStart
    temp.eval("""
              TripDurFromSec = SecEnd-SecStart
              DistOdomMi = (OdomFtEnd - OdomFtStart)/ 5280
              SpeedOdomMPH = (DistOdomMi/ TripDurFromSec) * 3600
              """,inplace=True)
      
    temp[["LatStart","LongStart","LatEnd","LongEnd"]] = temp[["LatStart","LongStart","LatEnd","LongEnd"]].astype(float)
    # Check what are units---Geopandas would be faster    
    # geometryStart = [Point(xy) for xy in zip(temp.LongStart, temp.LatStart)]
    # gdf=gpd.GeoDataFrame(geometry=geometryStart,crs={'init':'epsg:4326'})
    # geometryEnd = [Point(xy) for xy in zip(temp.LongEnd, temp.LatEnd)]
    # gdf2=gpd.GeoDataFrame(geometry=geometryEnd,crs={'init':'epsg:4326'})
    # distances = gdf.geometry.distance(gdf2)
    temp.loc[:,'CrowFlyDistLatLongMi'] = temp[["LatStart","LongStart","LatEnd","LongEnd"]].apply(lambda x: GetDistanceLatLong_mi(x[0],x[1],x[2],x[3]),axis=1)
    SummaryDat = taglineData.merge(temp,on= ['IndexTripStart','IndexTripEnd'],how ='left')
    SummaryDat.tag_date = SummaryDat.tag_date.astype(str)
    SummaryDat.loc[:,"StartDateTime"] = pd.to_datetime(SummaryDat['tag_date']+" "+SummaryDat['TripStartTime'])
    SummaryDat.loc[:,"EndDateTime"] = pd.to_datetime(SummaryDat['tag_date']+" "+SummaryDat['TripEndTime'], errors='coerce')
    SummaryDat.loc[:,"TripDurationFromTags"] = pd.to_timedelta(SummaryDat.loc[:,"EndDateTime"]- SummaryDat.loc[:,"StartDateTime"])
    SummaryDat.loc[:,"SpeedTripTagMPH"] = round(3600 * SummaryDat.DistOdomMi / SummaryDat.TripDurationFromTags.dt.total_seconds(),2)
    SummaryDat = SummaryDat[['fullpath', 'filename', 'file_busid', 'file_id', 'taglist','route_pattern', 'tag_busid',
                             'route', 'pattern', 'wday',
                             'StartDateTime','EndDateTime','IndexTripStart','IndexTripStartInCleanData','IndexTripEnd','IndexTripEndInCleanData','SecStart',
                 'OdomFtStart','SecEnd','OdomFtEnd',"TripDurFromSec","TripDurationFromTags",
                 "DistOdomMi", "SpeedOdomMPH", "SpeedTripTagMPH","CrowFlyDistLatLongMi"
                 ,"LatStart","LongStart","LatEnd","LongEnd"]]
    return(SummaryDat)
def RemoveAPC_CAL_Tags(data):
    #Remove all rows with "CAL" label
    MaskCal = data.loc[:,0].str.upper().str.strip() =="CAL"
    data = data[~MaskCal]
    #Remove APC tag and store tag location
    MaskAPC = data.loc[:,0].str.upper().str.strip() =="APC"
    APCTagLoc = np.array(data[MaskAPC].index)
    data = data[~MaskAPC]
    return(data, APCTagLoc)
    
def AddEndRouteInfo(data, taglineData):
    Pat = re.compile('^\s*/\s*(?P<TripEndTime>\d{2}:\d{2}:\d{2})\s*(?:Buswares navigation reported end of route|Buswares is now using route zero)',re.S) 
    data.loc[:,'TripEndTime']= data[0].str.extract(Pat)
    EndOfRoute = data[['IndexLoc','TripEndTime']]
    EndOfRoute = EndOfRoute[~(EndOfRoute.TripEndTime.isna())]
    deleteIndices = EndOfRoute.IndexLoc.values
    EndOfRoute.rename(columns={'IndexLoc':'IndexTripEnd'},inplace=True)
    EndOfRoute.IndexTripEnd = EndOfRoute.IndexTripEnd.astype('int32')
    taglineData.NewLineNo = taglineData.NewLineNo.astype('int32')
    EndOfRoute = pd.merge_asof(EndOfRoute,taglineData[['tag_time','NewLineNo']],left_on="IndexTripEnd",right_on='NewLineNo',direction='backward')
    EndOfRoute = EndOfRoute[~(EndOfRoute.duplicated(subset=['NewLineNo','tag_time'],keep='first'))]
    taglineData = taglineData.merge(EndOfRoute,on=['NewLineNo','tag_time'],how='left')
    taglineData.loc[:,'tempLine'] = taglineData['NewLineNo'].shift(-1)
    taglineData.loc[:,'tempTime'] = taglineData['tag_time'].shift(-1)
    taglineData.loc[taglineData.TripEndTime.isna(),['IndexTripEnd','TripEndTime']] = taglineData.loc[taglineData.TripEndTime.isna(),['tempLine','tempTime']].values
    if(np.isnan(taglineData.iloc[-1]['IndexTripEnd'])):
        taglineData.loc[taglineData.index.max(), 'IndexTripEnd'] = max(data.IndexLoc)
    taglineData.rename(columns={'tag_time':"TripStartTime"},inplace=True)
    return(taglineData, deleteIndices)
   
        
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

def GetDistanceLatLong_mi(Lat1, Long1, Lat2, Long2):
    distance_mi = geodesic((Lat1, Long1), (Lat2, Long2)).miles
    return(distance_mi)

def FindAllTags(ZipFolderPath, quiet = True):
    '''
    Parameters
    ----------
    ZipFolderPath: str
        Path to zipped folder with rawnav text file. 
        i.e., rawnav02164191003.txt.zip
        Assumes that included text file has the same name as the zipped file,
        minus the '.zip' extension.
        Note: For absolute paths, use forward slashes.

    Returns
    -------
    TagLineElements
        List of Character strings including line number, pattern, vehicle,
        date, and time
    '''
    if quiet != True: 
        print("Searching for tags in: " + ZipFolderPath)
 
    zf = zipfile.ZipFile(ZipFolderPath)
    # Get Filename
    namepat = re.compile('(rawnav\d+\.txt)') 
    ZipFileName = namepat.search(ZipFolderPath).group(1) 
    # Get Info
    infopat ='^\s*(\S+),(\d{1,5}),(\d{2}\/\d{2}\/\d{2}),(\d{2}:\d{2}:\d{2}),(\S+),(\S+)'
    TagLineElements = []
    TagLineNum = 1
    with io.TextIOWrapper(zf.open(ZipFileName, 'r'),encoding="utf-8") as input_file:
        for current_line in input_file:
            for match in re.finditer(infopat, current_line, re.S):
                # Turns out we don't really need capture groups
                # with string split approach, but leaving in for possible
                # future changes
                returnvals = str(TagLineNum) + "," + match.group()
                TagLineElements.append(returnvals)
            TagLineNum = TagLineNum + 1
            
    # WT: Not sure if necessary, may help with separating later
    # Python unnesting not as friendly as desired
    if len(TagLineElements) == 0:
         TagLineElements.append(',,,,,,')
                
    return(TagLineElements)
        
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
        

