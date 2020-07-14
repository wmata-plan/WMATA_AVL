# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:38:06 2020

@author: WylieTimmerman
"""
import pandas as pd 
import geopandas as gpd
from shapely.geometry import Point
from scipy.spatial import cKDTree
import numpy as np

def tribble(columns, *data):
    # I miss R
    return pd.DataFrame(
        data=list(zip(*[iter(data)]*len(columns))),
        columns=columns
    )

def check_convert_list(possible_list):
    if isinstance(possible_list,str):
        return ([possible_list])
    else:
        return (possible_list)
    
def drop_geometry(gdf):
    # Inspired by 
    # https://github.com/geopandas/geopandas/issues/544
    # and sf::st_drop_geometry
    # I miss R    
    df = pd.DataFrame(gdf[[col for col in gdf.columns if col != gdf._geometry_column_name]])
    
    return(df)
       
def explode_first_last(gdf):

    line_first_last_list = []
     
    # Not especially pythonic, but preserves dtypes nicely relative to itertuples and esp. iterrows
    for i in range(0,len(gdf)):
        justone = gdf.loc[[i],:]
    
        first_point = Point(list(justone['geometry'].iloc[0].coords)[0])
        last_point = Point(list(justone['geometry'].iloc[0].coords)[-1])
        
        first_row = gpd.GeoDataFrame(
            drop_geometry(justone).assign(location = 'first'),
            geometry = [first_point],
            crs = justone.crs)
        
        last_row = gpd.GeoDataFrame(
            drop_geometry(justone).assign(location = 'last'),
            geometry = [last_point],
            crs = justone.crs)

        line_first_last_list.append(first_row)
        line_first_last_list.append(last_row)
    
    line_first_last = gpd.GeoDataFrame( pd.concat( line_first_last_list, ignore_index=True, axis = 0),
                                       crs = line_first_last_list[0].crs)
    
    return(line_first_last)

def ckdnearest(gdA, gdB):
    """
    # https://gis.stackexchange.com/questions/222315/geopandas-find-nearest-point-in-other-dataframe
    Parameters
    ----------
    gdA : gpd.GeoDataFrame
        typically wmata schedule data for the correct route and direction.
    gdB : gpd.GeoDataFrame
        rawnav data: only nearest points to gdA are kept in the output.
    Returns
    -------
    gdf : gpd.GeoDataFrame
        wmata schedule data for the correct route and direction with the closest rawnav point.
    """
    gdA.reset_index(inplace=True, drop=True);
    gdB.reset_index(inplace=True, drop=True)
    nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)))
    nB = np.array(list(zip(gdB.geometry.x, gdB.geometry.y)))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdf = pd.concat(
        [gdA.reset_index(drop=True),
         gdB.loc[idx, ['filename', 'index_run_start', 'index_loc', 'lat', 'long']].reset_index(
             drop=True),
         pd.Series(dist, name='dist_to_nearest_point')], axis=1)
    return gdf
