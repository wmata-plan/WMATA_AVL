# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 10:18 2020
@author: abibeka
"""
# NOTE: To run tests, open terminal, activate environment, change directory
# to the repository, and then run
# pytest tests

# To run tests outside of terminal, set the current working directory
# to the repository outside of the script, ala
# import os
# os.chdir('C:\\OD\\OneDrive - Foursquare ITP\\Projects\\WMATA_AVL')
# Perhaps there's a way to tell pytest to run in a specific place or set sys
# paths to avoid this. Make sure that Spyder does not reset current working
# directory to the script location when running

import pytest
import os
import pandas as pd
import json
import glob
import sys

sys.path.append('.')

import wmatarawnav as wr

###############################################################################
# Load in data for testing
@pytest.fixture(scope="session")
def get_cwd():
    if os.getcwd().split('\\')[-1] == 'tests':
        os.chdir('../')
    return os.getcwd()


@pytest.fixture(scope="session")
def get_rawnav_clean_data(get_cwd):
    path_processed_route_data = os.path.join(get_cwd, "data/02-processed/RouteData")
    rawnav_dat = wr.read_processed_rawnav(
        analysis_routes_=["H8"],
        path_processed_route_data=path_processed_route_data,
        restrict=1000,
        analysis_days=["Monday", "Tueday"])
    rawnav_dat = wr.fix_rawnav_names(rawnav_dat)
    return rawnav_dat


@pytest.fixture(scope="session")
def get_rawnav_summary_dat(get_cwd):
    path_processed_route_data = 'C:\\Users\\abibeka\\OneDrive - Kittelson & Associates, Inc\\Documents\\Github\\WMATA_AVL\\data/02-processed/RouteData'
    path_processed_route_data = os.path.join(get_cwd, "data/02-processed/RouteData")
    rawnav_summary_dat, rawnav_trips_less_than_600sec_or_2miles = wr.read_summary_rawnav(
        analysis_routes_=["H8"],
        path_processed_route_data=path_processed_route_data,
        restrict=1000,
        analysis_days=["Monday", "Tueday"])
    rawnav_summary_dat = wr.fix_rawnav_names(rawnav_summary_dat)
    return rawnav_summary_dat

@pytest.fixture(scope="session")
def subset_valid_rawnav_trips(get_rawnav_clean_data, get_rawnav_summary_dat):
    rawnav_summary_dat = get_rawnav_summary_dat
    rawnav_dat =  get_rawnav_clean_data
    rawnav_summary_keys_col = rawnav_summary_dat[['filename', 'index_trip_start_in_clean_data']]
    # 2.3 Merge Processed and Summary data
    ############################################
    rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col, on=['filename', 'index_trip_start_in_clean_data'],
                                        how='right')
    return rawnav_qjump_dat

@pytest.fixture(scope="session")
def fix_data_type_issues_in_rawnav_qjump_dat(subset_valid_rawnav_trips):
    rawnav_qjump_dat = subset_valid_rawnav_trips
    rawnav_qjump_dat.pattern = rawnav_qjump_dat.pattern.astype('int')
    # Having issues with route "70" and "64"---Getting read as int instead of str
    rawnav_qjump_dat.route = rawnav_qjump_dat.route.astype(str)
    return rawnav_qjump_dat

@pytest.fixture(scope="session")
def fix_data_type_issues_in_rawnav_summary_dat(get_rawnav_summary_dat):
    rawnav_summary_dat = get_rawnav_summary_dat
    rawnav_summary_dat.route = rawnav_summary_dat.route.astype(str)
    return rawnav_summary_dat

@pytest.fixture(scope="session")
def get_wmata_schedule_data_q_jump_routes(get_cwd):
    path_wmata_schedule_data = os.path.join(get_cwd, "data/02-processed")
    wmata_schedule_data_file = os.path.join(path_wmata_schedule_data, 'wmata_schedule_data_q_jump_routes.csv')
    wmata_schedule_dat = wr.read_wmata_schedule(wmata_schedule_data_file=wmata_schedule_data_file)
    return wmata_schedule_dat

@pytest.fixture(scope="session")
def get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary(
        fix_data_type_issues_in_rawnav_qjump_dat, fix_data_type_issues_in_rawnav_summary_dat,
        get_wmata_schedule_data_q_jump_routes):
    rawnav_qjump_dat = fix_data_type_issues_in_rawnav_qjump_dat
    rawnav_summary_dat = fix_data_type_issues_in_rawnav_summary_dat
    wmata_schedule_dat = get_wmata_schedule_data_q_jump_routes
    wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
        wr.parent_merge_rawnav_wmata_schedule(
            analysis_route_=["H8"],
            analysis_day_=["Monday", "Tueday"],
            rawnav_dat_=rawnav_qjump_dat,
            rawnav_sum_dat_=rawnav_summary_dat,
            wmata_schedule_dat_=wmata_schedule_dat)
    return wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat

def test_rawnav_column_names(get_rawnav_clean_data):
    rawnav_dat = get_rawnav_clean_data
    found_column_names = rawnav_dat.columns
    expected_col_names = ['index_loc', 'lat', 'long', 'heading', 'door_state', 'veh_state', 'odomt_ft', 'sec_past_st',
                          'stop_window', 'row_before_apc', 'route_pattern', 'route', 'pattern',
                          'index_trip_start_in_clean_data', 'index_trip_end_in_clean_data', 'filename',
                          'start_date_time', 'wday']
    assert all(found_column_names == expected_col_names)


def test_rawnav_summary_column_names(get_rawnav_summary_dat):
    rawnav_summary_dat = get_rawnav_summary_dat
    found_column_names = rawnav_summary_dat.columns
    expected_col_names = ['fullpath', 'filename', 'file_busid', 'file_id', 'taglist', 'route_pattern', 'tag_busid',
                          'route', 'pattern', 'wday', 'start_date_time', 'end_date_time', 'index_trip_start',
                          'index_trip_start_in_clean_data', 'index_trip_end','index_trip_end_in_clean_data',
                          'sec_start', 'odom_ft_start', 'sec_end', 'odom_ft_end', 'trip_dur_from_sec',
                          'trip_duration_from_tags','dist_odom_mi', 'speed_odom_mph', 'speed_trip_tag_mph',
                          'crow_fly_dist_lat_long_mi', 'lat_start', 'long_start', 'lat_end', 'long_end', 'count1']
    assert all(found_column_names == expected_col_names)

def test_rawnav_summary_dat_trips_less_than_600sec_or_2miles(get_rawnav_summary_dat):
    rawnav_summary_dat = get_rawnav_summary_dat
    found_rows = rawnav_summary_dat.query('trip_dur_from_sec < 600 | dist_odom_mi < 2')
    found_num_rows = found_rows.shape[0]
    expected_num_rows = 0
    assert found_num_rows == expected_num_rows

def test_wmata_schedule_data_q_jump_routes_column_names(get_wmata_schedule_data_q_jump_routes):
    wmata_schedule_dat = get_wmata_schedule_data_q_jump_routes
    found_column_names = wmata_schedule_dat.columns
    expected_col_names = ['pattern_id', 'pattern_name', 'direction', 'trip_length', 'route','pattern',
                          'pattern_destination', 'route_text', 'route_key', 'direction_id', 'geo_id', 'stop_id',
                          'dist_from_previous_stop', 'order', 'stop_sort_order', 'geo_description', 'ta_geo_id',
                          'stop_lon', 'stop_lat', 'heading']
    assert all(found_column_names == expected_col_names)

def test_nearest_point_to_rawnav_col_names(get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary):
    wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
        get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary
    found_column_names = nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat.columns
    expected_col_names =['pattern_id', 'pattern_name', 'direction', 'trip_length', 'route','pattern',
                         'pattern_destination', 'route_text', 'route_key','direction_id', 'geo_id', 'stop_id',
                         'dist_from_previous_stop', 'order','stop_sort_order', 'geo_description','ta_geo_id','stop_lon',
                         'stop_lat', 'stop_heading', 'geometry', 'filename','index_trip_start_in_clean_data',
                         'index_loc', 'lat', 'long','dist_nearest_point_from_stop']
    assert all(found_column_names == expected_col_names)

def test_wmata_schedule_based_sum_dat_col_names(get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary):
    wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
        get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary
    found_column_names = wmata_schedule_based_sum_dat.columns
    expected_col_names =['start_odom_ft_wmata_schedule', 'end_odom_ft_wmata_schedule',
                         'trip_dist_mi_odom_and_wmata_schedule', 'start_sec_wmata_schedule','end_sec_wmata_schedule',
                         'trip_dur_sec_wmata_schedule','start_lat_wmata_schedule', 'end_lat_wmata_schedule',
                         'start_long_wmata_schedule', 'end_long_wmata_schedule','dist_first_stop_wmata_schedule',
                         'trip_length_mi_direct_wmata_schedule','route_text_wmata_schedule','pattern_name_wmata_schedule',
                         'direction_wmata_schedule','pattern_destination_wmata_schedule', 'direction_id_wmata_schedule',
                         'trip_speed_mph_wmata_schedule', 'file_busid', 'tag_busid','index_trip_start','index_trip_end',
                         'index_trip_end_in_clean_data','sec_start', 'odom_ft_start', 'sec_end', 'odom_ft_end',
                         'trip_dur_from_sec', 'trip_duration_from_tags', 'dist_odom_mi','speed_odom_mph',
                         'speed_trip_tag_mph', 'crow_fly_dist_lat_long_mi','lat_start', 'long_start', 'lat_end',
                         'long_end', 'count1']
    assert all(found_column_names == expected_col_names)


def test_stop_order_nearest_point_to_rawnav(get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary):
    wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
        get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary
    assert(all(nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat.
                groupby(['filename', 'index_trip_start_in_clean_data']).index_loc.diff().dropna() > 0))

def test_stop_dist_nearest_point_to_rawnav(get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary):
    wmata_schedule_based_sum_dat, nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat = \
        get_inventory_nearest_rawnav_point_to_stop_and_extended_trip_summary
    assert(all(nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat.dist_nearest_point_from_stop<=200))


