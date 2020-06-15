# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Created on Tue Apr 28 15:07:59 2020
Purpose: Functions for processing rawnav & wmata_schedule data
"""
import inflection
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.ops import nearest_points
from scipy.spatial import cKDTree
import numpy as np
import folium
from folium import plugins

# Eventually will clean the parse_rawnav.py functions to get these updated column names.
def fix_rawnav_names(data):
    if 'Unnamed: 0' in data.columns:
        data = data.drop(columns='Unnamed: 0')
    col_names = data.columns
    data.columns = [inflection.underscore(name) for name in col_names]
    return data


def merge_stops_gtfs_rawnav(wmata_schedule_dat, rawnav_dat):
    '''
    Parameters
    ----------
    wmata_schedule_dat : pd.DataFrame
        wmata schedule data with unique stops per route and info on short/long and direction.
    rawnav_dat : pd.DataFrame
        rawnav data.
    Returns
    -------
    nearest_rawnav_point_to_wmata_schedule_data : gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the GTFS stops on that route.
    '''
    # Convert to geopandas dataframe
    geometry_stops = [Point(xy) for xy in zip(wmata_schedule_dat.stop_lon.astype(float),
                                              wmata_schedule_dat.stop_lat.astype(float))]
    geometry_points = [Point(xy) for xy in zip(rawnav_dat.long.astype(float), rawnav_dat.lat.astype(float))]
    gd_wmata_schedule_dat =gpd.GeoDataFrame(wmata_schedule_dat, geometry=geometry_stops,crs={'init':'epsg:4326'})
    gd_rawnav_dat =gpd.GeoDataFrame(rawnav_dat, geometry=geometry_points,crs={'init':'epsg:4326'})
    # Project to 2-D plane
    #https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    gd_wmata_schedule_dat.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    gd_rawnav_dat.to_crs(epsg=3310,inplace=True) # Distance in meters---Default is in degrees!
    wmata_schedule_groups = gd_wmata_schedule_dat.groupby(['route','pattern']) # Group GTFS data
    rawnav_groups = gd_rawnav_dat.groupby(['filename','index_trip_start_in_clean_data','route','pattern']) # Group rawnav data
    nearest_rawnav_point_to_wmata_schedule_data =pd.DataFrame()
    for name, rawnav_group in rawnav_groups:
        wmata_schedule_relevant_route_dat =\
            wmata_schedule_groups.get_group((name[2],name[3])) #Get the relevant group in GTFS corresponding to rawnav.
        nearest_rawnav_point_to_wmata_schedule_data = \
            pd.concat([nearest_rawnav_point_to_wmata_schedule_data,
                       ckdnearest(wmata_schedule_relevant_route_dat, rawnav_group)])
    nearest_rawnav_point_to_wmata_schedule_data.dist =\
        nearest_rawnav_point_to_wmata_schedule_data.dist * 3.28084 # meters to feet
    nearest_rawnav_point_to_wmata_schedule_data.Lat =\
        nearest_rawnav_point_to_wmata_schedule_data.Lat.astype('float')
    geometry_nearest_rawnav_point = []
    for xy in zip(nearest_rawnav_point_to_wmata_schedule_data.long,
                  nearest_rawnav_point_to_wmata_schedule_data.lat):
        geometry_nearest_rawnav_point.append(Point(xy))
    geometry_stop_on_route = []
    for xy in zip(nearest_rawnav_point_to_wmata_schedule_data.stop_lon,
                  nearest_rawnav_point_to_wmata_schedule_data.stop_lat):
        geometry_stop_on_route.append(Point(xy))
    geometry = [LineString(list(xy)) for xy in zip(geometry_nearest_rawnav_point,geometry_stop_on_route)]
    nearest_rawnav_point_to_wmata_schedule_data=\
        gpd.GeoDataFrame(nearest_rawnav_point_to_wmata_schedule_data, geometry=geometry,crs={'init':'epsg:4326'})
    nearest_rawnav_point_to_wmata_schedule_data.rename(columns = {'dist':'dist_nearest_point_from_stop'},inplace=True)
    return nearest_rawnav_point_to_wmata_schedule_data

###################################################################################################################################

# ckdnearest
###################################################################################################################################
#https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
def ckdnearest(gdA, gdB):
    #TODO: Write Documentation
    '''
    Parameters
    ----------
    gdA : TYPE
        DESCRIPTION.
    gdB : TYPE
        DESCRIPTION.

    Returns
    -------
    gdf : TYPE
        DESCRIPTION.
    '''
    gdA.reset_index(inplace=True);gdB.reset_index(inplace=True)
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)) )
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)) )
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdf = pd.concat(
        [gdA.reset_index(drop=True), gdB.loc[idx, ['filename','IndexTripStartInCleanData','IndexLoc','Lat', 'Long']].reset_index(drop=True),
         pd.Series(dist, name='dist')], axis=1)
    return gdf