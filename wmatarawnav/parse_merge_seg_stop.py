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
     
    # Cleaning
    index_run_segment_start_end_1 =\
        remove_run_with_seg_dist_over_100ft(index_run_segment_start_end_1)
    
    #  - TODO: Are the nearest points in order, such that the segment start point has a lower index value than the
    #    segment end point (checks that the segment was drawn in the right direction and that any
    #    future bidirectional segment is actually drawn once for every segment)
    #  -  TODO: Are rawnav points continuously within a buffer of ~Y feet around a segment?
    #  - TODO: do we get first and last points for each run? later we filter on cases where we have
    #       both, but it'd be good to know in advance what we have.
    
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
        rawnav_q_dat.merge(first_last_stop_dat.drop(['odom_ft','sec_past_st'], axis = 1),
                           on=['filename', 'index_run_start'], 
                           how='right')
    rawnav_q_target_dat = rawnav_q_target_dat.query('index_loc>=index_loc_first_stop & index_loc<=index_loc_last_stop')
    # TODO: The points after this are largely copied and slimmed down from the schedule merge
    # implementation. Not ideal by any stretch, but making this more flexible would be a significant
    # chore given the variety of columns and aggregations that need to be applied in each case. 
    
    rawnav_q_target_dat = \
        rawnav_q_target_dat[
            ['filename',
             'index_run_start', 
             'seg_name_id', 
             'lat', 
             'long', 
             'heading', 
             'odom_ft', 
             'sec_past_st', 
             'first_stop_dist_nearest_point']]
    
    Map1 = lambda x: max(x) - min(x)
    rawnav_q_segment_summary = \
        rawnav_q_target_dat.groupby(['filename', 'index_run_start', 'seg_name_id']).\
            agg({'odom_ft': ['min', 'max', Map1],
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
                                                           'index_run_start'], 
                                       how='left')
                
    return rawnav_q_segment_summary

def remove_run_with_seg_dist_over_100ft(index_table):
    """
    Parameters
    ----------
    index_table: gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the wmata schedule stops on that route.
    Returns
    -------
    index_table: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to data where all stops with closest rawnav point > 100 ft.
        are removed.
    """
    row_before = index_table.shape[0]
    
    index_table_fil = (
        index_table.groupby(['filename','index_run_start'],sort = False)
                   .filter(lambda x: (x.dist_to_nearest_point < 100).all()))

    row_after = index_table_fil.shape[0]
    row_diff = row_before - row_after
    print('deleted {} rows from {} rows in index table where nearest points to rawnav both not <50 ft.'.format(row_diff, row_before))
    return index_table_fil
