# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:28:17 2020

@author: WylieTimmerman
"""

from . import parse_wmata_schedule as ws

def merge_rawnav_segment(rawnav_gdf_, 
                         rawnav_sum_dat_,
                         target_):
    """
    Parameters
    ----------
    rawnav_gdf_: gpd.GeoDataFrame, rawnav data
    rawnav_sum_dat_: pd.DataFrame, rawnav summary data
    target_:geopandas.geodataframe.GeoDataFrame, segments with geom for first last vertex
    Returns
    -------
    summary_run_segment: pd.DataFrame
        trip summary data with additional information from wmata schedule data
    index_run_segment_start_end: gpd.GeoDataFrame
    """
    # TODO: add error handling    
    
    # Find rawnav point nearest each segment
    index_run_segment_start_end = \
        ws.merge_rawnav_target(
            target_dat = target_,
            rawnav_dat = rawnav_gdf_)
    
    # WAIT FOR APOORBA TO IMPLEMENT THIS FIRST
    # I imagine we should do some double-checking that the results are about what we want for each run:
    #  - Are the points nearest to the end of the segment within ~ X feet?
    #  - Are the nearest points in order, such that the segment start point has a lower index value than the
    #    segment end point (checks that the segment was drawn in the right direction and that any
    #    future bidirectional segment is actually drawn once for every segment)
    #  -  Are rawnav points continuously within a buffer of ~Y feet around a segment?
    # At this point, one could further filter the rawnav_run_iteration_frame created in #2 to those 
    #   meeting certain criteria for quality (e.g., average speed not insanely high or low, total
    #   travel time or travel distance not crazy).
    
    # Generate Summary
    
    
    return(index_run_segment_start_end)
    
