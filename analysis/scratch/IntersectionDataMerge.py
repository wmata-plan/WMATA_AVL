# -*- coding: utf-8 -*-
"""
Created on Wed May 13 10:34:01 2020

@author: abibeka
"""
import numpy as np
import geopandas as gpd, os,sys
import pyarrow as pa
import pandas as pd
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.ops import nearest_points
from scipy.spatial import cKDTree
import pyarrow.parquet as pq
path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
os.chdir(os.path.join(path_working))
sys.path.append(path_working) 

# Source data
path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
GTFS_Dir = os.path.join(path_source_data, "google_transit")   
# Processed data
path_processed_data = os.path.join(path_source_data,"ProcessedData")

IntersectionData = gpd.read_file(os.path.join(path_source_data,'Intersection_Points-shp'))
IntersectionData1 = IntersectionData[['FULLINTERS','geometry']]

RawnavData = pq.read_table(source =os.path.join(path_processed_data,"Route79_Partition.parquet")).to_pandas()
RawnavData.drop(columns="__index_level_0__",inplace=True)
FinSummaryDat = pd.read_excel(os.path.join(path_processed_data,'GTFSTripSummaries.xlsx'))
file= 'rawnav02806191007.txt'
IndxStLoc ="6227"
FinSummaryDat = FinSummaryDat.query("filename==@file & IndexTripStartInCleanData == @IndxStLoc")
FinSummaryDat = FinSummaryDat.iloc[0,:]
RawnavData1 = RawnavData.query("filename==@file & IndexTripStartInCleanData == @IndxStLoc")

Temp = mergeIntersectionRawnav(IntersectionData1, RawnavData1)
Temp1 = Temp.query('dist<100')
IntData = IntersectionData1
Temp1.loc[:,'stop_sequence'] = 0
Temp1.loc[:,'stop_name'] = 0
Temp1.loc[:,'direction_id'] = 0
Temp2 =Temp1.copy()
IntersectionData.rename(columns= {'geometry':"geometryInt"},inplace=True)
Temp2 = Temp2.merge(IntersectionData,on =['FULLINTERS'],how='left')
Temp2 = Temp2[["FULLINTERS",'OBJECTID', 'MARID', 'INTERSECTI',
       'STREET1ID', 'STREET2ID', 'ST1NAME', 'ST1TYPE', 'ST1QUAD', 'FULLSTREET',
       'ST2NAME', 'ST2TYPE', 'ST2QUAD', 'FULLSTRE_1', 'REFX', 'REFY',
       'NATIONALGR', 'STREET1SEG', 'STREET2SEG', 'NODEID', 'INTERSEC_1',
       'SOURCE', 'LATITUDE', 'LONGITUDE', 'geometryInt']]
Temp2= gpd.GeoDataFrame(Temp2, geometry = Temp2.geometryInt)
Temp2.drop(columns="geometryInt",inplace=True)
outPath = os.path.join(path_processed_data,"IntersectionRoute79")
if not os.path.exists(outPath): os.makedirs(outPath)
Temp2.to_file( filename= os.path.join(outPath,"IntersectionRoute79.shp"))

SaveFile= f"test.html"
wr.PlotRawnavTrajWithGTFS(RawnavData1, Temp1,path_processed_data,SaveFile)


rawnavDat = RawnavData1
def mergeIntersectionRawnav(IntData, rawnavDat):
    geometryPoints = [Point(xy) for xy in zip(rawnavDat.Long.astype(float), rawnavDat.Lat.astype(float))]
    gdA = IntData
    gdB =gpd.GeoDataFrame(rawnavDat, geometry=geometryPoints,crs={'init':'epsg:4326'})
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