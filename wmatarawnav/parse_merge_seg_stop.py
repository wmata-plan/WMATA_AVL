# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:28:17 2020

@author: WylieTimmerman
"""

def parent_merge_rawnav_segment(rawnav_dat_, 
                                rawnav_sum_dat_,
                                segments_):
    """
    Parameters
    ----------
    rawnav_dat_: pd.DataFrame, rawnav data
    rawnav_sum_dat_: pd.DataFrame, rawnav summary data
    segment:geopandas.geodataframe.GeoDataFrame, segments
    Returns
    -------
    summary_run_segment: pd.DataFrame
        trip summary data with additional information from wmata schedule data
    index_run_segment_start_end: gpd.GeoDataFrame
    """
    
    # if (rawnav_sum_subset_dat.shape[0] == 0): return None, None
    
    # # Find rawnav point nearest each stop
    # index_run_segment_start_end = \
    # merge_rawnav_segment(
    #     wmata_schedule_dat = segment,
    #     rawnav_dat = rawnav_subset_dat)
    
    print('himom')
    
