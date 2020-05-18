# -*- coding: utf-8 -*-
"""
Created on Sun May 17 09:51:12 2020
Purpose create time-space diagrams for Route 79 Piney branch Segment
@author: abibeka
"""



#0.0 Housekeeping. Clear variable space
from IPython import get_ipython  #run magic commands
ipython = get_ipython()
# ipython.magic("reset -f")
# ipython = get_ipython()

#1 Import Libraries
########################################################################################
import pandas as pd, os, numpy as np, pyproj, sys, zipfile, glob, logging
from datetime import datetime
from geopy.distance import geodesic
from collections import defaultdict
from shapely.geometry import Point
from shapely.geometry import LineString
from scipy.spatial import cKDTree
import pyarrow as pa
import pyarrow.parquet as pq
import geopandas as gpd
from scipy import stats
import plotly.express as px
import plotly.graph_objects as go
#Using Plotly with Spyder
#https://community.plot.ly/t/plotly-for-spyder/10527/2
from plotly.offline import plot

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore") #Too many Pandas warnings

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
AnalysisRoutes = ['79']
ZipParentFolderName = "October 2019 Rawnav"

#1 Analyze Route ---Subset RawNav Data. 
########################################################################################
FinDat = pq.read_table(source =os.path.join(path_processed_data,"Route79_Partition.parquet")).to_pandas()
FinDat.route = FinDat.route.astype('str')
FinDat.columns
#Check for duplicate IndexLoc
assert(FinDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].count().values.max()==1)


FinDat.loc[:,"Count"] = FinDat.groupby(['filename','IndexTripStartInCleanData','IndexLoc'])['IndexLoc'].transform("count")
FinDatCheck = FinDat[FinDat.Count>1] # Check what is happening with the file here :'rawnav06435191012.txt' on Friday.
FinDatCheck.filename.unique()
FinDat = FinDat.query("Count==1")
# 1.1 Summary Data
########################################################################################
FinSummaryDat = pd.read_csv(os.path.join(path_processed_data,'TripSummaries.csv'))
FinSummaryDat.IndexTripStartInCleanData = FinSummaryDat.IndexTripStartInCleanData.astype('int32')
FinSummaryDat.route_pattern = FinSummaryDat.route_pattern.str.strip()
FinSummaryDat_79_01 = FinSummaryDat.query('route_pattern =="7901"')
FinSummaryDat_79_01 = FinSummaryDat_79_01.query('DistOdomMi>=4  & DistOdomMi<=10')
FinSummaryDat_79_01 = FinSummaryDat_79_01[['filename','IndexTripStartInCleanData']]
FinDat2 = FinSummaryDat_79_01.merge(FinDat,on= ['filename','IndexTripStartInCleanData'],how='left')
# 2 Get filter trajectory
########################################################################################
PineyBranchData = pd.DataFrame({"Pineylat":[38.968452,38.963196],"Pineylong":[-77.027389,-77.027872],"pos":["start","end"]})
geometryPoints = [Point(xy) for xy in zip(FinDat2.Long.astype(float), FinDat2.Lat.astype(float))]    
geometryPiney = [Point(xy) for xy in zip(PineyBranchData.Pineylong.astype(float), PineyBranchData.Pineylat.astype(float))]
FinDat2 =gpd.GeoDataFrame(FinDat2, geometry=geometryPoints,crs={'init':'epsg:4326'})
PineyBranchData =gpd.GeoDataFrame(PineyBranchData, geometry=geometryPiney,crs={'init':'epsg:4326'})

FilterDat = mergePineySegRawnav(PineyBranchData, FinDat2)
FilterDat.query('dist<70').dist.describe()
FilterDat.loc[:,'FilterDist'] = FilterDat.groupby(['filename','IndexTripStartInCleanData'])['dist'].transform("max")
Check = FilterDat.query('FilterDist>70')
FilterDat1 = FilterDat.query('FilterDist<70')
FilterDat1St = FilterDat1.query('pos=="start"')
geodesic = pyproj.Geod(ellps='WGS84')
FilterDat1St.loc[:,"Bearing"] = FilterDat1St.apply(lambda x:geodesic.inv(x.Pineylat,x.Pineylong, x.Lat, x.Long)[0],axis=1)
FilterDat1St.loc[:,'dist'] = FilterDat1St.dist * (-1)*(FilterDat1St.Bearing)/ abs(FilterDat1St.Bearing)
FilterDat1St = FilterDat1St.eval("SubtractOdom = OdomtFt+ dist")
FilterDat1St = FilterDat1St[['filename', 'IndexTripStartInCleanData','IndexLoc','dist','SubtractOdom',"SecPastSt"]]\
    .rename(columns={'IndexLoc':"IndexLocFilterLower",'dist':'dist_Upper',"SecPastSt":"SubtractSec"})


FilterDat1End = FilterDat1.query('pos=="end"')[['filename', 'IndexTripStartInCleanData','IndexLoc','dist']].rename(columns={'IndexLoc':"IndexLocFilterUpper",'dist':'dist_lower'})
FilterDat2 = FilterDat1St.merge(FilterDat1End,on =['filename', 'IndexTripStartInCleanData'],how='left')
FinDat3 = FilterDat2.merge( FinDat2, on=['filename', 'IndexTripStartInCleanData'],how='left')
FinDat3 = FinDat3.query('IndexLocFilterLower<=IndexLoc<=IndexLocFilterUpper')

#Check if the merging is correct
FinDat3.groupby(['filename', 'IndexTripStartInCleanData']).\
    agg({'IndexLoc':['min','max'],'IndexLocFilterLower':'mean',
         'IndexLocFilterUpper':'mean'}).apply(lambda x:x[('IndexLoc','min')]-x['IndexLocFilterLower']+\
                                              x[('IndexLoc','max')]-x['IndexLocFilterUpper'],axis=1).sum()

FinDat3.loc[:,'hour'] = FinDat3.StartDateTime.dt.hour
HourInterval = pd.IntervalIndex.from_tuples([(0, 5), (6, 10), (11, 13),
                                     (14,16),(17,19),(20,24)],closed= "both")
FinDat3.loc[:,"HourIntevals"] = pd.cut(FinDat3.hour,HourInterval)
FinDat3.loc[:,'Weekend-Weekday'] = FinDat3.wday.apply(lambda x: "weekend" if x in ['Saturday','Sunday'] else "weekday")
FinDat3 = FinDat3.eval("OdomAdjustedFt=OdomtFt-SubtractOdom")
FinDat3 = FinDat3.eval("TimeAdjustedSec=SecPastSt-SubtractSec")
FinDat3.loc[:,"UniqueId"] = FinDat3.filename+FinDat3.IndexTripStartInCleanData.astype(str)
test = FinDat3.head(100)
fig3 = px.line(FinDat3, x = "TimeAdjustedSec", y = "OdomAdjustedFt",
               line_group='UniqueId' ,color ="HourIntevals",line_dash="wday",facet_col ="Weekend-Weekday"
               ,template="plotly_white"
               , title = "SpaceTimeDia")
fig3.update_layout(showlegend=True)
plot(fig3, filename=os.path.join(path_processed_data,"SpaceTimeDia.html"),auto_open=True)

def mergePineySegRawnav(PineyBranchData, rawnavDat):
    gdB = rawnavDat
    gdA = PineyBranchData
    #https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    gdA.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    gdB.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    TripGroups = gdB.groupby(['filename','IndexTripStartInCleanData','route'])
    #GTFS_groups = gdA.groupby('route_id')
    NearestRawnavOnGTFS =pd.DataFrame()
    for name, groupRawnav in TripGroups:
        print(name)
        NearestRawnavOnGTFS = pd.concat([NearestRawnavOnGTFS,\
                                         ckdnearest(gdA,groupRawnav)])
    NearestRawnavOnGTFS.dist = NearestRawnavOnGTFS.dist * 3.28084 # meters to feet
    NearestRawnavOnGTFS.Lat =NearestRawnavOnGTFS.Lat.astype('float')
    #NearestRawnavOnGTFS.loc[:,"CheckDist"] = NearestRawnavOnGTFS.apply(lambda x: geopy.distance.geodesic((x.Lat,x.Long),(x.stop_lat,x.stop_lon)).meters,axis=1)
    geometry1 = [Point(xy) for xy in zip(NearestRawnavOnGTFS.Long, NearestRawnavOnGTFS.Lat)]
    NearestRawnavOnGTFS.to_crs(epsg =4326,inplace=True)
    geometry2 =NearestRawnavOnGTFS.geometry
    geometry = [LineString(list(xy)) for xy in zip(geometry1,geometry2)]
    NearestRawnavOnGTFS=gpd.GeoDataFrame(NearestRawnavOnGTFS, geometry=geometry,crs={'init':'epsg:4326'})
    return(NearestRawnavOnGTFS)

#https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
def ckdnearest(gdA, gdB):
    gdA.reset_index(inplace=True,drop=True);gdB.reset_index(inplace=True,drop=True)
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)) )
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)) )
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdf = pd.concat(
        [gdA.reset_index(drop=True), gdB.loc[idx, gdB.columns != 'geometry'].reset_index(drop=True),
         pd.Series(dist, name='dist')], axis=1)
    return gdf







