# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:38:06 2020

@author: WylieTimmerman
"""
import pandas as pd 
import geopandas as gpd
from shapely.geometry import Point

def tribble(columns, *data):
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
     
    # Note especially pythonic, but preserves dtypes nicely relative to itertuples and esp. iterrows
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
    