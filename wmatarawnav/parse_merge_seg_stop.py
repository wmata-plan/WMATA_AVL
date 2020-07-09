# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:28:17 2020

@author: WylieTimmerman
"""

from . import parse_wmata_schedule as ws
from . import low_level_fns as ll
import pyarrow as pa
import pyarrow.parquet as pq

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
    index_run_segment_start_end_1 =\
        ws.merge_rawnav_target(
            target_dat = target_,
            rawnav_dat = rawnav_gdf_)
    
    index_run_segment_start_end = ll.drop_geometry(index_run_segment_start_end_1)
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
    summary_run_segment =\
        include_segment_summary(
            rawnav_q_dat = rawnav_gdf_,
            rawnav_sum_dat = rawnav_sum_dat_,
            nearest_stop_dat = index_run_segment_start_end_1) #later replace with corrected vers
    
    # Could do this earlier, but need to remove geometry reference in get_first_last_stop_rawnav if so
    index_run_segment_start_end = ll.drop_geometry(index_run_segment_start_end_1)
      
    return(index_run_segment_start_end, summary_run_segment)
    
def include_segment_summary(rawnav_q_dat, 
                            rawnav_sum_dat, 
                            nearest_stop_dat):
    '''
    Parameters
    ----------
    rawnav_q_dat: pd.DataFrame, rawnav data
    rawnav_sum_dat: pd.DataFrame, rawnav summary data
    nearest_stop_dat: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to  where
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed i.e. stops are
            removed if  order does not increase with index_loc or time or distance.
            - where all stops with closest rawnav point > 100 ft. are removed.
    Returns
    -------
    rawnav_q_stop_sum_dat: pd.DataFrame
        trip summary data with additional information from wmata schedule data
    '''
    # 5 Get summary after merging files
    ########################################################################################
    first_last_stop_dat = ws.get_first_last_stop_rawnav(nearest_stop_dat)
    rawnav_q_target_dat = \
        rawnav_q_dat.merge(first_last_stop_dat, on=['filename', 'index_trip_start_in_clean_data'], how='right')
    rawnav_q_target_dat = rawnav_q_target_dat.query('index_loc>=index_loc_first_stop & index_loc<=index_loc_last_stop')
    # TODO: The points after this are largely copied and slimmed down from the schedule merge
    # implementation. Not ideal by any stretch, but making this more flexible would be a significant
    # chore given the variety of columns and aggregations that need to be applied in each case. 
    rawnav_q_target_dat = \
        rawnav_q_target_dat[
            ['filename',
             'index_trip_start_in_clean_data', 
             'seg_name_id', 
             'lat', 
             'long', 
             'heading', 
             'odomt_ft', 
             'sec_past_st', 
             'first_stop_dist_nearest_point']]
    
    Map1 = lambda x: max(x) - min(x)
    rawnav_q_segment_summary = \
        rawnav_q_target_dat.groupby(['filename', 'index_trip_start_in_clean_data', 'seg_name_id']).\
            agg({'odomt_ft': ['min', 'max', Map1],
                 'sec_past_st': ['min', 'max', Map1],
                 'lat': ['first', 'last'],
                 'long': ['first', 'last'],
                 'first_stop_dist_nearest_point': ['first']})

    rawnav_q_segment_summary.columns = ['start_odom_ft_segment', 
                                     'end_odom_ft_segment',
                                     'trip_dist_mi_odom_and_segment', 
                                     'start_sec_segment',
                                     'end_sec_segment', 
                                     'trip_dur_sec_segment',
                                     'start_lat_segment', 
                                     'end_lat_segment',
                                     'start_long_segment', 
                                     'end_long_segment',
                                     'dist_first_stop_segment'] 
    rawnav_q_segment_summary.loc[:, ['trip_dist_mi_odom_and_segment']] = \
        rawnav_q_segment_summary.loc[:, ['trip_dist_mi_odom_and_segment']] / 5280
    rawnav_q_segment_summary.loc[:, 'trip_speed_mph_segment'] = \
        round(3600 *
              rawnav_q_segment_summary.trip_dist_mi_odom_and_segment /
              rawnav_q_segment_summary.trip_dur_sec_segment, 2)
    rawnav_q_segment_summary.loc[:, ['trip_dist_mi_odom_and_segment', 
                                  'dist_first_stop_segment']] = \
        round(rawnav_q_segment_summary.loc[:, ['trip_dist_mi_odom_and_segment', 
                                            'dist_first_stop_segment']], 2)
    
    rawnav_q_segment_summary.reset_index(inplace = True)
        
    rawnav_q_segment_summary =\
        rawnav_q_segment_summary.merge(rawnav_sum_dat, on=['filename', 
                                                           'index_trip_start_in_clean_data'], 
                                       how='left')
                
    return rawnav_q_segment_summary
