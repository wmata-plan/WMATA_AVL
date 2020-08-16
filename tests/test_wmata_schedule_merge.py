# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 10:18 2020
@author: abibeka
"""
# NOTE: To run tests, open terminal, activate environment, change directory
# to the repository, and then run
# pytest tests

# To run tests outside of terminal, set the current working directory
# to the repository outside of the script

import pytest
import os
import sys
import pandas as pd
import geopandas as gpd
sys.path.append('.')

import wmatarawnav as wr

#Fixme: fix the tests
###############################################################################
# Load in data for testing
@pytest.fixture(scope="session")
def get_cwd():
    if os.getcwd().split('\\')[-1] == 'tests':
        os.chdir('../') # assume accidentally set to script directory
    return os.getcwd()

@pytest.fixture(scope="session")
def get_rawnav_summary_dat(get_cwd):

    path_parquet = os.path.join(
        get_cwd,  
        "data",
        "00-raw",
        "demo_data",
        "02_notebook_data")
    
    rawnav_summary_dat = wr.read_cleaned_rawnav(
        analysis_routes_=["H8"],
        analysis_days_=["Sunday"],
        path=os.path.join(path_parquet,"rawnav_summary_demo.parquet")
    )
    
    rawnav_summary_dat = rawnav_summary_dat.query('not (run_duration_from_sec < 600 | dist_odom_mi < 2)')

    return rawnav_summary_dat

@pytest.fixture(scope="session")
def get_rawnav_data(get_cwd,get_rawnav_summary_dat):
    
    path_parquet = os.path.join(
        get_cwd,  
        "data",
        "00-raw",
        "demo_data",
        "02_notebook_data")

    rawnav_dat = wr.read_cleaned_rawnav(
        analysis_routes_=["H8"],
        analysis_days_=["Sunday"],
        path=os.path.join(path_parquet,"rawnav_data_demo.parquet")
    )
    
    rawnav_summary_keys_col = get_rawnav_summary_dat[['filename', 'index_run_start']]
            
    rawnav_qjump_dat = rawnav_dat.merge(rawnav_summary_keys_col,
                                        on=['filename', 'index_run_start'],
                                        how='right')
    
    rawnav_qjump_gdf = (
        gpd.GeoDataFrame(
            rawnav_qjump_dat,
            geometry=gpd.points_from_xy(rawnav_qjump_dat.long, rawnav_qjump_dat.lat),
            crs='EPSG:4326'
        )
        .to_crs(epsg=2248)
    )
    
    return rawnav_qjump_gdf

@pytest.fixture(scope="session")
def get_wmata_schedule_data(get_cwd):
    wmata_schedule_dat = (
        pd.read_csv(
            os.path.join(
                get_cwd, 
                "data",
                "00-raw",
                "demo_data",
                "02_notebook_data",
                "wmata_schedule_data_q_jump_routes.csv"
            ),
            index_col = 0
        )
        .reset_index(drop=True)
    )
    
    wmata_schedule_gdf = (
        gpd.GeoDataFrame(
            wmata_schedule_dat, 
            geometry = gpd.points_from_xy(wmata_schedule_dat.stop_lon,wmata_schedule_dat.stop_lat),
            crs='EPSG:4326')
        .to_crs(epsg=2248)
    )   
    
    return wmata_schedule_gdf

@pytest.fixture(scope="session")
def get_stops_results(get_rawnav_data, get_rawnav_summary_dat, get_wmata_schedule_data):

    stop_summary, stop_index = (
        wr.merge_rawnav_wmata_schedule(
            analysis_route_=["H8"],
            analysis_day_=["Sunday"],
            rawnav_dat_=get_rawnav_data,
            rawnav_sum_dat_=get_rawnav_summary_dat,
            wmata_schedule_dat_=get_wmata_schedule_data
        )
    )
    return(stop_summary, stop_index)

# Tests
######

def test_stop_order_nearest_point_to_rawnav(get_stops_results):
    stop_summary, stop_index = get_stops_results
    
    each_stop_seq_more_than_last = all(
        stop_index
        .groupby(['filename', 'index_trip_start_in_clean_data'])
        .index_loc
        .diff()
        .dropna() 
        > 0
    )
    
    assert(each_stop_seq_more_than_last)

def test_stop_dist_nearest_point_to_rawnav(get_stops_results):
    stop_summary, stop_index = get_stops_results
    each_stop_close_enough = all(stop_index.dist_nearest_point_from_stop<=100)
    assert(each_stop_close_enough)

def test_nearest_point(get_stops_results):
    # These points were visually verified with diagnostic maps, then baked into this test
    stop_summary, stop_index = get_stops_results
    
    case_file = "rawnav03236191021.txt"
    case_run = 21537

    nearest_pt = (
        stop_index
        .query('filename == @case_file & index_run_start == @case_run & stop_id == 2368')
    )
    
    nearest_lat_match = (
        nearest_pt
        .lat
        .pipe(round,2)
        .eq(38.93)
        .all()
    )
    
    assert(nearest_lat_match)
    
    nearest_long_match = (
        nearest_pt
        .long
        .pipe(round,2)
        .eq(-77.04)
        .all()
    )
    
    assert(nearest_long_match)

    
