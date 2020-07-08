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
    index_run_segment_start_end_1 =\
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
    summary_run_segment =\
        include_segment_summary(
            rawnav_q_dat = rawnav_gdf_,
            rawnav_sum_dat = rawnav_sum_dat_,
            nearest_stop_dat = index_run_segment_start_end_1) #later replace with corrected vers
    
    
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
    breakpoint() # see how the above turns out 
    rawnav_q_target_dat = \
        rawnav_q_dat.merge(first_last_stop_dat, on=['filename', 'index_trip_start_in_clean_data'], how='right')
    rawnav_q_target_dat = rawnav_q_target_dat.query('index_loc>=index_loc_first_stop & index_loc<=index_loc_last_stop')
    rawnav_q_target_dat = \
        rawnav_q_target_dat[
            ['filename', 'index_trip_start_in_clean_data', 'lat', 'long', 'heading', 'odomt_ft', 'sec_past_st'
                , 'first_stop_dist_nearest_point', 'trip_length', 'route_text', 'pattern_name', 'direction',
             'pattern_destination', 'direction_id']]
    Map1 = lambda x: max(x) - min(x)
    rawnav_q_stop_sum_dat = \
        rawnav_q_target_dat.groupby(['filename', 'index_trip_start_in_clean_data']). \
            agg({'odomt_ft': ['min', 'max', Map1],
                 'sec_past_st': ['min', 'max', Map1],
                 'lat': ['first', 'last'],
                 'long': ['first', 'last'],
                 'first_stop_dist_nearest_point': ['first'],
                 'trip_length': ['first'],
                 'route_text': ['first'],
                 'pattern_name': ['first'],
                 'direction': ['first'],
                 'pattern_destination': ['first'],
                 'direction_id': ['first']})
    breakpoint() # i think the points after this are scehdule specific, right?
    # COULD maybe insert a string here to make this reusable...
    # need to replace trip with run
    rawnav_q_stop_sum_dat.columns = ['start_odom_ft_wmata_schedule', 'end_odom_ft_wmata_schedule',
                                     'trip_dist_mi_odom_and_wmata_schedule', 'start_sec_wmata_schedule',
                                     'end_sec_wmata_schedule', 'trip_dur_sec_wmata_schedule',
                                     'start_lat_wmata_schedule', 'end_lat_wmata_schedule',
                                     'start_long_wmata_schedule', 'end_long_wmata_schedule',
                                     'dist_first_stop_wmata_schedule', 'trip_length_mi_direct_wmata_schedule',
                                     'route_text_wmata_schedule', 'pattern_name_wmata_schedule',
                                     'direction_wmata_schedule', 'pattern_destination_wmata_schedule',
                                     'direction_id_wmata_schedule']
    rawnav_q_stop_sum_dat.loc[:, ['trip_dist_mi_odom_and_wmata_schedule']] = \
        rawnav_q_stop_sum_dat.loc[:, ['trip_dist_mi_odom_and_wmata_schedule']] / 5280
    rawnav_q_stop_sum_dat.loc[:, ['trip_length_mi_direct_wmata_schedule']] = \
        rawnav_q_stop_sum_dat.loc[:, ['trip_length_mi_direct_wmata_schedule']] / 5280
    rawnav_q_stop_sum_dat.loc[:, 'trip_speed_mph_wmata_schedule'] = \
        round(3600 *
              rawnav_q_stop_sum_dat.trip_dist_mi_odom_and_wmata_schedule /
              rawnav_q_stop_sum_dat.trip_dur_sec_wmata_schedule, 2)
    rawnav_q_stop_sum_dat.loc[:, ['trip_dist_mi_odom_and_wmata_schedule', 'dist_first_stop_wmata_schedule',
                                  'trip_length_mi_direct_wmata_schedule']] = \
        round(rawnav_q_stop_sum_dat.loc[:, ['trip_dist_mi_odom_and_wmata_schedule', 'dist_first_stop_wmata_schedule',
                                            'trip_length_mi_direct_wmata_schedule']], 2)
    rawnav_q_stop_sum_dat = \
        rawnav_q_stop_sum_dat.merge(rawnav_sum_dat, on=['filename', 'index_trip_start_in_clean_data'], how='left')
        
    rawnav_q_stop_sum_dat.set_index(
        ['fullpath', 'filename', 'file_id', 'wday', 'start_date_time', 'end_date_time',
         'index_trip_start_in_clean_data', 'taglist', 'route_pattern', 'route', 'pattern'], 
        inplace=True,
        drop=True)
        
    return rawnav_q_stop_sum_dat