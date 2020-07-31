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
                         target_,
                         patterns_by_seg_):
    """
    Parameters
    ----------
    rawnav_gdf_: gpd.GeoDataFrame, rawnav data
    rawnav_sum_dat_: pd.DataFrame, rawnav summary data
    target_:geopandas.geodataframe.GeoDataFrame, segments with geom for first last vertex
    patterns_by_seg_: pd.DataFrame, crosswalk of route and pattern to seg_name_id
    Returns
    -------
    summary_run_segment: pd.DataFrame
        trip summary data with additional information from wmata schedule data
    index_run_segment_start_end: gpd.GeoDataFrame
    """

    assert(len(target_) == 1), print("Function expects a segments file with one record")

    # Measure original shape for later testing.
    # Note that we default to the 2248 EPSG code (unit: feet) for measurement testing, 
    # which may be inappropriate if this code is used in other locations.
    seg_length = (
        target_
        .to_crs(2248)
        .geometry
        .length
        .iloc[0] #return a float
    )
    
    # Subset segment shapes to current segment and add route identifier
    seg_pattern_shape = (
        target_
        # Add route and pattern identifier
        .merge(patterns_by_seg_,
               on = ['seg_name_id'],
               how = "left")
    )
    
    # Prepare segment shape for merge    
    seg_pattern_first_last = ll.explode_first_last(seg_pattern_shape)
           
    # Find rawnav point nearest each segment
    index_run_segment_start_end_1 = (
        ws.merge_rawnav_target(
            target_dat = seg_pattern_first_last,
            rawnav_dat = rawnav_gdf_
        )
    )
     
    # Cleaning
    index_run_segment_start_end_1 =\
        remove_runs_with_too_far_pings(index_run_segment_start_end_1)
    
    #  - TODO: Are the nearest points in order, such that the segment start point has a lower index value than the
    #    segment end point (checks that the segment was drawn in the right direction and that any
    #    future bidirectional segment is actually drawn once for every segment)
    #  -  TODO: Are rawnav points continuously within a buffer of ~Y feet around a segment?
    #  - TODO: Are the odometer readings increasing as expected? Can sometimes get odometer 
    #      advancing 3x as fast as it should.
    #  - TODO: do we get first and last points for each run? later we filter on cases where we have
    #       both, but it'd be good to know in advance what we have.


    # Generate Summary
    summary_run_segment = (
        include_segment_summary(
            rawnav_q_dat = rawnav_gdf_,
            rawnav_sum_dat = rawnav_sum_dat_,
            nearest_stop_dat = index_run_segment_start_end_1
        )
    )
    
    # Additional Checks
    summary_run_segment = remove_runs_with_too_long_odom(summary_run_segment, seg_length)
    
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
            - stops whose ordering does not correspond to the index_loc/ time/ odometer are removed 
            i.e. stops are removed if  order does not increase with index_loc or time or distance.
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
    rawnav_q_target_dat = (
        rawnav_q_target_dat
        .query('index_loc>=index_loc_first_stop & index_loc<=index_loc_last_stop')
    )
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

def remove_runs_with_too_far_pings(index_table,
                                  threshold_ft = 50):
    """
    Parameters
    ----------
    index_table: gpd.GeoDataFrame
        A geopandas dataframe with nearest rawnav point to each of the segment start or endpoints.
    Returns
    -------
    index_table: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to data where all stops with closest
        rawnav point > threshold are removed.
    """
    row_before = index_table.shape[0]
    
    index_table_fil = (
        index_table.groupby(['filename','index_run_start'],sort = False)
                   .filter(lambda x, y = threshold_ft: (x.dist_to_nearest_point < y).all()))

    row_after = index_table_fil.shape[0]
    row_diff = row_before - row_after    
    
    if (row_diff > 0):
        print(
            'deleted {} rows from {} rows in index table where nearest points to rawnav both not <{} ft.'
            .format(row_diff, row_before, round(threshold_ft))
        )
    
    return index_table_fil

def remove_runs_with_too_long_odom(summary_table,
                                   seg_length_,
                                   threshold_ft = 150):
    """
    Parameters
    ----------
    summary_table: pd.DataFrame
        A dataframe with one row for each rawnav trip
    Returns
    -------
    summary_table: pd.DataFrame
        A dataframe with one row for each rawnav trip
    Notes
    -----
    We want to to ensure that no segment is too long -- occassionally odometers will advance 3X as fast as 
    expected. 150 ft is a semi-arbitrary leeway to account for not all pings falling exactly at
    segment boundaries, additional odometer mileage that some routes will experience.
    """
    
    row_before = summary_table.shape[0]

    summary_table = (
        summary_table
        .loc[
                (summary_table.trip_dist_mi_odom_and_segment < ((seg_length_ + threshold_ft)/5280)) &
                (summary_table.trip_dist_mi_odom_and_segment > ((seg_length_ - threshold_ft)/5280))
            ]
    )
    
    row_after = summary_table.shape[0]
    row_diff = row_before - row_after
    if (row_diff > 0):
        print(
            'deleted {} runs from {} runs in summary table where run length not within {} ft of segment length {} ft'
            .format(row_diff, row_before, threshold_ft, round(seg_length_))
        )
    
    return(summary_table)

    
