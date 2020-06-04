# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Created on Thu Mar 26 10:09:45 2020
Purpose: Functions for processing rawnav data
"""

import zipfile,re,numpy as np, pandas as pd\
    ,folium, io, os, shutil, glob
import pandasql as ps
from zipfile import BadZipfile
import geopandas as gpd
from shapely.geometry import Point

#Parent Functions
#################################################################################################################
# GetZippedFilesFromZipDir
#########################################################################################
def GetZippedFilesFromZipDir(ZipDirList,ZippedFilesDirParent,globSearch = "*.zip"):
    '''
    Get the list of files to read from Zipped folder. Also Unzip the parent folder.
    Will Unzip only once. Can also pass a list of paths to unzipped folders
    to achieve the same result.
    Parameters
    ----------
    ZipDirList : List or str, 
        List of zipped directories with Rawnav data
        or a single zipped directory with Rawnav data.
    ZippedFilesDirParent: str
        Parent folder where list of zipped directories with Rawnav data is kept.
        List of zipped directories with Rawnav data would be unzipped to this parent folder.
    globSearch: str
        Default value of "*.zip" will search for all .zip files. Can specify a pariticular file
        name pattern for debugging. 
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
        listFiles = glob.glob(os.path.join(ZipDir1,globSearch))
        FileUniverse.extend(listFiles)
    return(FileUniverse)
#########################################################################################
#find_rawnav_routes
#########################################################################################           
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
    # Note that we leave line_num as text, as integer values don't support
    # NAs in Pandas
    FileUniverseDF['tag_busid'] = pd.to_numeric(FileUniverseDF['tag_busid'])
    FileUniverseDF['tag_datetime'] = FileUniverseDF['tag_date'] + ' ' + FileUniverseDF['tag_time']
    FileUniverseDF['tag_datetime'] = pd.to_datetime(FileUniverseDF['tag_datetime'], infer_datetime_format=True, errors = 'coerce')
    FileUniverseDF['tag_starthour'] = FileUniverseDF['tag_time'].str.extract('^(?:\s*)(\d{2}):')
    FileUniverseDF['tag_starthour'] = pd.to_numeric(FileUniverseDF['tag_starthour'])
    FileUniverseDF['tag_date'] = pd.to_datetime(FileUniverseDF['tag_date'], infer_datetime_format=True)
    FileUniverseDF['wday'] = FileUniverseDF['tag_date'].dt.day_name()
    return(FileUniverseDF)
#########################################################################################
# load_rawnav_data
#########################################################################################
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
#########################################################################################
# clean_rawnav_data
#########################################################################################
def clean_rawnav_data(DataDict, filename): 
    '''
    Parameters
    ----------
    DataDict : dict
        dict of raw data and the data on tag lines.
    Returns
    -------
    Cleaned data without any tags.
    '''    
    rawnavdata = DataDict['RawData']
    taglineData = DataDict['tagLineInfo']
    # Check the location of taglines from taglineData data match the locations in rawnavdata
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
        print("TagLists Did not match")
    #Keep index references. Will use later
    rawnavdata.reset_index(inplace=True); rawnavdata.rename(columns = {"index":"IndexLoc"},inplace=True)
    #Get End of route Info
    taglineData, deleteIndices1 = AddEndRouteInfo(rawnavdata, taglineData)
    rawnavdata = rawnavdata[~rawnavdata.index.isin(np.append(TagIndices,deleteIndices1))]
    #Remove APC and CAL labels and keep APC locations. Can merge_asof later.
    rawnavdata, APCTagLoc = RemoveAPC_CAL_Tags(rawnavdata)
    CheckDat = rawnavdata[~rawnavdata.apply(CheckValidDataEntry,axis=1)]
    #Remove the other remaining tags. 
    Pat = re.compile('.*/\s*(((\d{2}:\d{2}:\d{2})\s*(Buswares.*SHUTDOWN|bwrawnav collection.*))|collection stopped.*)',re.S|re.I) 
    assert(sum(~(CheckDat[0].str.match(Pat)))==0), print(f"Did not handle some additional lines in CheckDat. Check file {filename}")
    rawnavdata = rawnavdata[rawnavdata.apply(CheckValidDataEntry,axis=1)]
    #Add the APC tag to the rawnav data to identify stops
    APClocDat = pd.Series(APCTagLoc,name='APCTagLoc')
    APClocDat = pd.merge_asof(APClocDat,rawnavdata[["IndexLoc"]],left_on = "APCTagLoc",right_on="IndexLoc") # default direction is backward
    rawnavdata.loc[:,'RowBeforeAPC'] = False
    rawnavdata.loc[APClocDat.IndexLoc,'RowBeforeAPC'] = True
    taglineData.rename(columns={'NewLineNo':"IndexTripStart"},inplace=True)
    #Get trip summary
    SummaryData = GetTripSummary(data= rawnavdata,taglineData = taglineData)
    ColumnNmMap = {0:'Lat',1:'Long',2:'Heading',3:'DoorState',4:'VehState',5:'OdomtFt',6:'SecPastSt',7:'SatCnt',
                   8:'StopWindow',9:'Blank',10:'LatRaw',11:'LongRaw'}
    rawnavdata.rename(columns =ColumnNmMap,inplace=True )
    #Add composite key to the data
    rawnavdata = AddTripDividers(rawnavdata,SummaryData )
    rawnavdata.loc[:,"filename"] = filename
    returnDict = {'rawnavdata':rawnavdata,'SummaryData':SummaryData}
    return(returnDict)
#########################################################################################
# subset_rawnav_trip1
#########################################################################################
def subset_rawnav_trip(RawnavDataDict_, rawnav_inventory_filtered_, AnalysisRoutes_):
    '''
    Subset data for analysis routes
    Parameters
    ----------
    RawnavDataDict_ : dict
        Cleaned data without any tags. filename is the dictionary key.
    rawnav_inventory_filtered_ : pd.DataFrame
        DataFrame with file details to any file including at least one of our analysis routes 
        Note that other non-analysis routes will be included here, but information about these routes
        is currently necessary to split the rawnav file correctly.
    AnalysisRoutes_ : list
        list of routes that need to be subset.
    Returns
    -------
    FinDat : pd.DataFrame
        Concatenated data for an analysis route. 
    '''
    FinDat = pd.DataFrame()
    SearchDF = rawnav_inventory_filtered_[['route','filename']].set_index('route')
    RouteFiles = np.unique((SearchDF.loc[AnalysisRoutes_,:].values).flatten())
    FinDat = pd.concat([RawnavDataDict_[file] for file in RouteFiles])
    FinDat.reset_index(drop=True,inplace=True)
    FinDat = FinDat.query("route in @AnalysisRoutes_")
    return(FinDat)
#########################################################################################
# subset_summary_data
#########################################################################################
def subset_summary_data(FinSummaryDat_, AnalysisRoutes_):
    '''
    Parameters
    ----------
    FinSummaryDat_ : pd.DataFrame
        Summary data that has columns on trip start and end lat-longs.
    AnalysisRoutes_ : list
        list of routes that need to be subset.
    Returns
    -------
    SumData:pd.DataFrame
        Subset of summary data with analysis routes only. 
    SumData_StartGpd : gpd.GeoDataFrame
            Subset of summary data with analysis routes only and start point used for geometry column. 
    SumData_EndGpd : gpd.GeoDataFrame
        Subset of summary data with analysis routes only and end point used for geometry column. 
    '''
    SumData = FinSummaryDat_.query("route in @AnalysisRoutes_")
    SumData.reset_index(drop=True,inplace=True)
    tempDf = SumData[['filename','IndexTripStartInCleanData','LatStart', 'LongStart','route']]
    geometryStart = [Point(xy) for xy in zip(tempDf.LongStart, tempDf.LatStart)]
    SumData_StartGpd=gpd.GeoDataFrame(tempDf, geometry=geometryStart,crs={'init':'epsg:4326'})
    tempDf=None
    tempDf = SumData[['filename','IndexTripStartInCleanData','LatEnd', 'LongEnd','route']]
    geometryEnd = [Point(xy) for xy in zip(tempDf.LongEnd, tempDf.LatEnd)]
    SumData_EndGpd=gpd.GeoDataFrame(tempDf, geometry=geometryEnd,crs={'init':'epsg:4326'})
    return(SumData,SumData_StartGpd,SumData_EndGpd)
#########################################################################################

#Nested Functions
#################################################################################################################
# AddTripDividers
#########################################################################################
def AddTripDividers(data, SummaryData):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        rawnav data without tags.
    SummaryData : pd.DataFrame
        Tagline data.
    Returns
    -------
    rawnav data with composite keys.

    '''
    SummaryData.columns
    TagsTemp = SummaryData[['route_pattern','route', 'pattern','IndexTripStartInCleanData','IndexTripEndInCleanData']]
    q1  = '''SELECT data.IndexLoc,data.Lat,data.Long,data.Heading,data.DoorState,data.VehState,data.OdomtFt,data.SecPastSt,
        data.SatCnt,data.StopWindow,data.Blank,data.LatRaw,data.LongRaw,data.RowBeforeAPC,
        TagsTemp.route_pattern,TagsTemp.route,TagsTemp.pattern,TagsTemp.IndexTripStartInCleanData,TagsTemp.IndexTripEndInCleanData
        FROM data LEFT JOIN TagsTemp on data.IndexLoc BETWEEN  TagsTemp.IndexTripStartInCleanData and TagsTemp.IndexTripEndInCleanData
    '''
    data = ps.sqldf(q1, locals())
    return(data)    
#########################################################################################   
# GetTripSummary
#########################################################################################
def GetTripSummary(data, taglineData):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        rawnav data without tags.
    taglineData : pd.DataFrame
        Tagline data.
    Returns
    -------
    Summary data : pd.DataFrame
        Data with trip level summary. 
    '''
    temp = taglineData[['IndexTripStart','IndexTripEnd']]
    temp = temp.astype('int32')
    rawDaCpy = data[['IndexLoc',0,1,5,6]].copy()
    rawDaCpy[['IndexLoc',5,6]]= rawDaCpy[['IndexLoc',5,6]].astype('int32')
    rawDaCpy.rename(columns={0:'Lat',1:'Long',5:'OdomtFt',6:'SecPastSt'},inplace=True)
    #Get rows with trip start from rawDaCpy
    temp = pd.merge_asof(temp,rawDaCpy,left_on = "IndexTripStart",right_on ="IndexLoc" , direction='forward')
    temp.rename(columns = {'Lat':"LatStart",'Long':"LongStart",
                           'OdomtFt': "OdomFtStart",'SecPastSt':"SecStart","IndexLoc":"IndexTripStartInCleanData"},inplace=True)
    #Get rows with trip end from rawDaCpy
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
    #Get distance b/w trip start and end lat-longs
    temp.loc[:,'CrowFlyDistLatLongMi'] = GetDistanceLatLong_mi(temp,"LatStart","LongStart","LatEnd","LongEnd")
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
#########################################################################################
# RemoveAPC_CAL_Tags
#########################################################################################
def RemoveAPC_CAL_Tags(data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        Unclean data with tag information.
    Returns
    -------
    data : pd.DataFrame
        data without APC and CAL tags. 
    APCTagLoc : np.array
        Location of APC tags. Used to create a new column about bus door open status. 
    '''
    #Remove all rows with "CAL" label
    MaskCal = data.loc[:,0].str.upper().str.strip() =="CAL"
    data = data[~MaskCal]
    #Remove APC tag and store tag location
    MaskAPC = data.loc[:,0].str.upper().str.strip() =="APC"
    APCTagLoc = np.array(data[MaskAPC].index)
    data = data[~MaskAPC]
    return(data, APCTagLoc)
#########################################################################################
# AddEndRouteInfo
#########################################################################################
def AddEndRouteInfo(data, taglineData):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        Unclean data with info on end of route.
    taglineData : pd.DataFrame
        Tagline data.
    Returns
    -------
    taglineData : pd.DataFrame
        Tagline data with trip end time adjusted based on
        "Busware navigation reported end of route..." or
        "Buswares is now using route zero" tags.
    deleteIndices : np.array
        indices to delete from raw data.
    '''
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
######################################################################################### 
# GetDistanceLatLong_mi
#########################################################################################
def GetDistanceLatLong_mi(Data,Lat1,Long1,Lat2,Long2):
    '''
    Parameters
    ----------
    Data : pd.DataFrame
        Any dataframe.
    Lat1 : str
        1st lat column.
    Long1 : str
        1st long column.
    Lat2 : str
        2nd lat column.
    Long2 : str
        2nd long column.
    Returns
    -------
    DistanceMi: np.array
        distances in mile between (Lat1,Long1) and (Lat2,Long2) columns in Data.
        same size as number of rows in Data.
    '''
    geometry1 = [Point(xy) for xy in zip(Data[Long1], Data[Lat1])]
    gdf=gpd.GeoDataFrame(geometry=geometry1,crs={'init':'epsg:4326'})
    gdf.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    geometry2 = [Point(xy) for xy in zip(Data[Long2], Data[Lat2])]
    gdf2=gpd.GeoDataFrame(geometry=geometry2,crs={'init':'epsg:4326'})
    gdf2.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    #https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    DistanceMi = gdf.geometry.distance(gdf2) * 0.000621371 # meters to miles    
    return(DistanceMi.values)
#########################################################################################
# GetDistanceLatLong_ft_fromGeom
#########################################################################################
def GetDistanceLatLong_ft_fromGeom(geometry1, geometry2):
    '''
    Parameters
    ----------
    geometry1 : pd.series of shapely.geometry.point
    geometry2 : pd.series of shapely.geometry.point
        same size as geometry1
    Returns
    -------
    DistanceFt: np.array
    distances in feet between each points in geometry1 and geometry2.
    same size as geometry1 or geometry2. 
    '''
    gdf=gpd.GeoDataFrame(geometry=geometry1,crs={'init':'epsg:4326'})
    gdf.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    gdf2=gpd.GeoDataFrame(geometry=geometry2,crs={'init':'epsg:4326'})
    gdf2.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    #https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    DistanceFt = gdf.geometry.distance(gdf2) * 3.28084 # meters to feet    
    return(DistanceFt.values)
#########################################################################################
# FindAllTags
#########################################################################################
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
    try:
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
        if len(TagLineElements) == 0:
             TagLineElements.append(',,,,,,')
    except BadZipfile as BadZipEr:
        print("*"*100)
        print(f"Issue with opening zipped file: {ZipFolderPath}. Error: {BadZipEr}")
        print("*"*100)
        TagLineElements = []
        TagLineElements.append(',,,,,,')
    except KeyError as keyerr:
        print("*"*100)
        print(f"Text file name doesn't match parent zip folder for': {ZipFolderPath}. Error: {keyerr}")
        print("*"*100)
        TagLineElements = []
        TagLineElements.append(',,,,,,')
    return(TagLineElements)
#########################################################################################
# MoveEmptyIncorrectLabelFiles
#########################################################################################   
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
#########################################################################################
# is_numeric
#########################################################################################
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
#########################################################################################
# CheckValidDataEntry
#########################################################################################
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
#########################################################################################

