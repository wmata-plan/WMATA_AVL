# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:28:17 2020

@author: WylieTimmerman
"""

from . import merge_schedule_stops as ws
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
        .merge(
            patterns_by_seg_,
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

    # Note that while we could run some additional checks (Are *both* ends of the segment
    # present? Does the run stay 'within' a certain radius of the segment line?) these 
    # checks are largely superseded by checks that the odometer reading approximately matches
    # the segment length (done further below). 
    index_run_segment_start_end_2 = (
        index_run_segment_start_end_1
        .assign(flag_too_far = lambda x: x.dist_to_nearest_point > 50)
        # note that we will give both the whole run the 'wrong order' flag in the summary table
        # if the order test fails for any point
        # This wrong order flag is necessary because some early login and late close out runs
        # will have pings around certain segments, resulting in misshapen joins. This could 
        # be addressed if we spent more time cleaning up those runs, but instead we just drop 
        # them through these filters.
        .assign(
            flag_wrong_order = lambda x: 
                x
                .groupby(['filename','index_run_start'], sort = False)
                .index_loc
                .diff()
                .fillna(0)
                .lt(0) 
        )
    )
        
    # Generate Summary
    summary_run_segment = (
        include_segment_summary(
            rawnav_q_dat = rawnav_gdf_,
            rawnav_sum_dat = rawnav_sum_dat_,
            nearest_seg_boundary_dat = index_run_segment_start_end_2,
            seg_length_ = seg_length
        )
    )

    # Could do this earlier, but need to remove geometry reference in get_first_last_stop_rawnav if so
    index_run_segment_start_end = ll.drop_geometry(index_run_segment_start_end_2)
      
    return(index_run_segment_start_end, summary_run_segment)
    
def include_segment_summary(rawnav_q_dat, 
                            rawnav_sum_dat, 
                            nearest_seg_boundary_dat,
                            seg_length_):
    """
    Parameters
    ----------
    rawnav_q_dat: pd.DataFrame, rawnav data
    rawnav_sum_dat: pd.DataFrame, rawnav summary data
    nearest_seg_boundary_dat: gpd.GeoDataFrame
        cleaned data on nearest rawnav point to where segment boundary lies
    seg_length_: float
        length of segment in feet
    Returns
    -------
    rawnav_q_segment_summary: pd.DataFrame
        run summary data with additional information from segment data
    Notes
    -----
    This function is largely copied and slimmed down from the schedule merge
    implementation. Making this function more flexible to accommodate both cases would be a significant
    investment given the variety of columns and aggregations that need to be applied in each case.     
    """
    seg_boundary_dat = ws.get_first_last_stop_rawnav(nearest_seg_boundary_dat)
    rawnav_q_target_dat = (
        rawnav_q_dat
        .merge(seg_boundary_dat
               .drop(['odom_ft','sec_past_st'], axis = 1),
               on=['filename', 'index_run_start'], 
               how='right'
        )
    )
    
    rawnav_q_target_dat = (
        rawnav_q_target_dat
        .query('index_loc>=index_loc_first_stop & index_loc<=index_loc_last_stop')
    )

    rawnav_q_target_dat = (
        rawnav_q_target_dat
        .filter(
            items = 
                ['filename',
                 'index_run_start', 
                 'seg_name_id', 
                 'index_loc',
                 'lat', 
                 'long', 
                 'heading', 
                 'odom_ft', 
                 'sec_past_st', 
                 'first_stop_dist_nearest_point'
                ]
        )
    )
    
    # We'll add some additional calcs to check on the odometer values within the segment
    rawnav_q_target_dat[['odom_ft_next','sec_past_st_next']] = (
        rawnav_q_target_dat
        .groupby(['filename','index_run_start','seg_name_id'], sort = False)[['odom_ft','sec_past_st']]
        .shift(-1)
    )
        
    rawnav_q_target_dat = (
        rawnav_q_target_dat
        .assign(
            secs_marg=lambda x: x.sec_past_st_next - x.sec_past_st,
            odom_ft_marg=lambda x: x.odom_ft_next - x.odom_ft
        )
    )
    
    Map1 = lambda x: max(x) - min(x)
    rawnav_q_segment_summary = (
        rawnav_q_target_dat
        .groupby(['filename', 'index_run_start', 'seg_name_id'])
        .agg(
            {'odom_ft': ['min', 'max', Map1, 'sum'],
             'sec_past_st': ['min', 'max', Map1, 'sum'],
             'lat': ['first', 'last'],
             'long': ['first', 'last'],
             'first_stop_dist_nearest_point': ['first'],
             'index_loc' : ['first','last']}
        )
    )

    rawnav_q_segment_summary.columns = ['start_odom_ft_segment', 
                                     'end_odom_ft_segment',
                                     'trip_dist_ft_segment', 
                                     'odom_ft_marg_sum',
                                     'start_sec_segment',
                                     'end_sec_segment', 
                                     'trip_dur_sec_segment',
                                     'secs_marg_sum',
                                     'start_lat_segment', 
                                     'end_lat_segment',
                                     'start_long_segment', 
                                     'end_long_segment',
                                     'dist_first_stop_segment',
                                     'start_index_loc_segment',
                                     'end_index_loc_segment'] 
    
    rawnav_q_segment_summary = (
        rawnav_q_segment_summary
        .assign(
            trip_dist_mi_odom_and_segment = lambda x: x.trip_dist_ft_segment / 5280,
            trip_speed_mph_segment = lambda x:
                round(3600 * x.trip_dist_mi_odom_and_segment / x.trip_dur_sec_segment, 2)
        )
        .assign(
            trip_dist_mi_odom_and_segment = lambda x: round(x.trip_dist_mi_odom_and_segment, 2),
            dist_first_stop_segment = lambda x: round(x.dist_first_stop_segment, 2)
        )
        .reset_index()
    )
    
    # Add flags at summary level
    # For example, we occassionally see the odometer reset in teh middle of a run
    # see 'rawnav05447191026.txt' and index_run_start 8055. If this occurred in a segment
    # it could affect decomposition calculations
    # Runs with these flags will be removed before downstream calculations are performed
    rawnav_q_segment_summary = (
        rawnav_q_segment_summary
        .assign(
            flag_too_long_odom = lambda x, y = seg_length_:
                abs(y - (x.trip_dist_ft_segment)) > 150,
            secs_total_mismatch = lambda x: 
                ((x.end_sec_segment - x.start_sec_segment) 
                 - x.trip_dur_sec_segment),
            flag_secs_total_mismatch = lambda x: x.secs_total_mismatch != 0,
            odom_total_mismatch = lambda x:
                (x.end_odom_ft_segment - x.start_odom_ft_segment ) 
                 - (x.trip_dist_ft_segment),
            flag_odom_total_mismatch = lambda x: x.odom_total_mismatch != 0
        )
        .drop(
            columns = [
                'secs_marg_sum',
                'odom_ft_marg_sum',
                'secs_total_mismatch',
                'odom_total_mismatch',
                'trip_dist_ft_segment'
                ]
            )
    )
    # Summarize index-level flags
    flags = (
        nearest_seg_boundary_dat
        .groupby(['filename','index_run_start'])
        .agg({'flag_too_far':['any'],
              'flag_wrong_order':['any']})
        .pipe(ll.reset_col_names)
    )
    
    # Merge and column clean up
    rawnav_q_segment_summary = (
        rawnav_q_segment_summary
        .merge(
            rawnav_sum_dat, # we start with the original summary to not silently drop runs
            on=['filename', 'index_run_start'], 
            how='left'
        )
        .merge(
            flags,
            on=['filename', 'index_run_start'], 
            how='left'
        )
        .pipe(
            ll.reorder_first_cols,
            first_cols_list = [
                'filename',
                'index_run_start',
                'seg_name_id',
                'route',
                'pattern',
                'start_date_time',
                'wday',
                'flag_too_far_any',
                'flag_wrong_order_any',
                'flag_too_long_odom',
                'flag_secs_total_mismatch',
                'flag_odom_total_mismatch',
                'start_odom_ft_segment', 
                'end_odom_ft_segment',
                'trip_dist_mi_odom_and_segment'
                ]
        )
    )
        
    return rawnav_q_segment_summary

    
