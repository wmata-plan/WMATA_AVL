# -*- coding: utf-8 -*-
"""
Created on Mon May  4 21:41:20 2020

@author: abibeka
"""


# Geopandas LineString

CleanDataDict.keys()
CheckDat = CleanDataDict['rawnav00008191007.txt']['summary_data']
CheckDat.columns
geometry1 = [Point(xy) for xy in zip(CheckDat.LongStart, CheckDat.LatStart)]
geometry2 = [Point(xy) for xy in zip(CheckDat.LongEnd, CheckDat.LatEnd)]
geometry = [LineString(list(xy)) for xy in zip(geometry1,geometry2)]
gdf2=gpd.GeoDataFrame(CheckDat, geometry=geometry,crs={'init':'epsg:4326'})
gdf2.to_crs(epsg=3310,inplace=True)

distances = gdf2.geometry.length *0.000621371
with pd.option_context('display.max_rows', None, 'display.max_columns', 40):  # more options can be specified also
    print(CheckDat)
CheckDat['']



