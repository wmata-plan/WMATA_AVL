# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:20:18 2020

@author: WylieTimmerman, Apoorba Bibeka
"""

import pyarrow.parquet as pq
from . import low_level_fns as ll


def read_cleaned_rawnav(analysis_routes_, path_processed_route_data, restrict, analysis_days_):
    """
    Parameters
    ----------
    analysis_routes_: list,
        routes for which rawnav data is needed. Should be a subset of following:
        ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                         'H8','W47']
    path_processed_route_data: str,
       path where the parquet files for cleaned data is kept
    restrict: None, if all rawnav files need to be searched
    analysis_days_: list,
        days of the week for which data is needed. Should be a subset of following:
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    Returns
    -------
    rawnav_dat: pd.DataFrame,
      rawnav data
    """
    
    # Parameter Checks
    # Convert back to list if not already
    analysis_routes_ = ll.check_convert_list(analysis_routes_)    
    analysis_days_ = ll.check_convert_list(analysis_days_)
    
    day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    assert (set(analysis_days_).issubset(set(day_of_week))),\
        print("""analysis_days_ is a subset of following days: 
              ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
              """)
    assert (len(analysis_days_) == len(set(analysis_days_))),\
        print("analysis_days_ entries cannot be duplicated")     
         
    # Function Body
    rawnav_temp_list = []
    rawnav_dat = pd.DataFrame()
    
    for analysis_route in analysis_routes_:
        filter_parquet = [[('wday', '=', day)] for day in analysis_days_]
        try:
            rawnav_temp_dat = \
                pq.read_table(source=os.path.join(path_processed_route_data,
                                                  f"Route{analysis_route}_Restrict{restrict}.parquet"),
                              filters=filter_parquet).to_pandas()
        except Exception as e:
            if str(type(e)) == "<class 'IndexError'>":
                raise ValueError('No data found for given filter conditions')
            else:
                print(e)
                raise
        else:
            # NOTE: some of this due to conversion to and from parquet, weird things can happen
            rawnav_temp_dat.route = rawnav_temp_dat.route.astype('str')
            rawnav_temp_dat.drop(columns=['Blank', 'LatRaw', 'LongRaw', 'SatCnt', '__index_level_0__'], 
                                 inplace=True)

            rawnav_temp_list.append(rawnav_temp_dat)
    rawnav_dat = pd.concat(rawnav_temp_list)
    return rawnav_dat

def read_summary_rawnav(analysis_routes_, path_processed_route_data, restrict, analysis_days_):
    """
    Parameters
    ----------
    analysis_routes_: list,
        routes for which rawnav data is needed. Should be a subset of following:
        ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                         'H8','W47']
    path_processed_route_data: str,
       path where the parquet files for cleaned data is kept
    restrict: None, if all rawnav files need to be searched
    analysis_days_: list,
        days of the week for which data is needed. Should be a subset of following:
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    Returns
    -------
    rawnav_summary_dat: pd.DataFrame
        rawnav summary data
    rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat: pd.DataFrame
        rows that are removed from summary data.
    """
    # Parameter Checks
    
    # Convert to analysis_routes back to list if not already
    analysis_routes_ = ll.check_convert_list(analysis_routes_)    
    analysis_days_ = ll.check_convert_list(analysis_days_)


    day_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    assert (set(analysis_days_).issubset(set(day_of_week))),\
        print("""analysis_days_ is a subset of following days: 
              ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
              """)
    assert (len(analysis_days_) == len(set(analysis_days_))),\
        print("analysis_days_ entries cannot be duplicated")       

    # Function Body
    
    rawnav_temp_list = []
    rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_list = []
    rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat = pd.DataFrame()
    rawnav_summary_dat = pd.DataFrame()
 
    for analysis_route in analysis_routes_:
        temp_rawnav_sum_dat = \
            pd.read_csv(os.path.join(path_processed_route_data,
                                     f'TripSummaries_Route{analysis_route}_Restrict{restrict}.csv'))
        total_rows_read = temp_rawnav_sum_dat.shape[0]
        temp_rawnav_sum_dat.IndexTripStartInCleanData = temp_rawnav_sum_dat.IndexTripStartInCleanData.astype('int32')
        temp_rawnav_sum_dat = temp_rawnav_sum_dat.query('wday in @analysis_days_')
        if temp_rawnav_sum_dat.shape[0] == 0:
            raise ValueError(f"No trips on any of the analysis_days_ ({analysis_days_})")
        temp_rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat = \
            temp_rawnav_sum_dat.query('TripDurFromSec < 600 | DistOdomMi < 2')
        temp_rawnav_sum_dat = temp_rawnav_sum_dat.query('not (TripDurFromSec < 600 | DistOdomMi < 2)')
        rows_removed_in_above_query = temp_rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat.shape[0]
        print(f'Removing {rows_removed_in_above_query} out of {total_rows_read} trips/ rows with TripDurFromSec < 600'
              f' seconds or DistOdomMi < 2 miles from route {analysis_route}')
        rawnav_temp_list.append(temp_rawnav_sum_dat)
        rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_list.append(
            temp_rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat)
    rawnav_summary_dat = pd.concat(rawnav_temp_list)
    rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat = \
        pd.concat(rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_list)
    return rawnav_summary_dat, rawnav_tripdur_less_than_600sec_dist_odom_less_than_2mi_dat

