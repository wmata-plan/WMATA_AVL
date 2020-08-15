# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 06:26:18 2020

@author: WylieTimmerman
"""

# NOTE: To run tests, open terminal, activate environment, change directory
# to the repository, and then run 
# pytest tests

import pytest
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import json
import glob
import sys

sys.path.append('.')
import wmatarawnav as wr


###############################################################################
# Load in data for testing
@pytest.fixture(scope="session")
def get_cwd():
    if os.getcwd().split('\\')[-1]== 'tests':
        os.chdir('../')
    return os.getcwd()

@pytest.fixture(scope="session")
def get_analysis_routes():
    analysis_routes = ['H8']
    return(analysis_routes)
    
@pytest.fixture(scope="session")
def get_analysis_days():
    analysis_days = ['Sunday']
    return(analysis_days)

@pytest.fixture(scope="session")
def get_rawnav_gdf(get_analysis_routes,get_analysis_days,get_cwd):

    rawnav_dat = (
        wr.read_cleaned_rawnav(
            analysis_routes_=get_analysis_routes,
            analysis_days_=get_analysis_days,
            path = os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "02_notebook_data",
                "rawnav_data_demo.parquet"
            )
        )
    )
    
    rawnav_qjump_gdf = (
        gpd.GeoDataFrame(
            rawnav_dat, 
            geometry = gpd.points_from_xy(
                rawnav_dat.long,
                rawnav_dat.lat
            ),
            crs='EPSG:4326')
        .to_crs(epsg=2248)
    )
    
    return(rawnav_qjump_gdf)

@pytest.fixture(scope="session")
def get_summary(get_analysis_routes,get_analysis_days,get_cwd):
    
    rawnav_summary_dat = (
        wr.read_cleaned_rawnav(
            analysis_routes_ = get_analysis_routes,
            analysis_days_ = get_analysis_days,
            path = os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "02_notebook_data",
                "rawnav_summary_demo.parquet"
            )
        )
    )
    return(rawnav_summary_dat)

@pytest.fixture(scope="session")
def get_segments(get_cwd):
    segments = (
        gpd.read_file(
            os.path.join(
                get_cwd,
                "data",
                "02-processed",
                "segments.geojson"
            )
        )
        .to_crs(2248)
    )
    
    return(segments)

# @pytest.fixture(scope="session")
def get_wmata_schedule_gdf():
    breakpoint()
    # wmata_schedule_dat = (
    #     pd.read_csv(
    #         os.path.join(
    #             get_cwd,
    #             "data",
    #             "00-raw",
    #             "demo_data",
    #             "02_notebook_data",
    #             "wmata_schedule_data_q_jump_routes.csv"
    #         ),
    #         index_col = 0
    #     )
    #     .reset_index(drop=True)
    # )

    # wmata_schedule_gdf = (
    #     gpd.GeoDataFrame(
    #         wmata_schedule_dat, 
    #         geometry = gpd.points_from_xy(wmata_schedule_dat.stop_lon,wmata_schedule_dat.stop_lat),
    #         crs='EPSG:4326')
    #     .to_crs(epsg=2248)
    # )
    
    # return(wmata_schedule_gdf)

@pytest.fixture(scope="session")
def get_patterns():
    patterns = (
        pd.DataFrame(
            {'route':['H8'],
             'pattern':[2],
             'seg_name_id':['irving_fifteenth_sixteenth_stub']}
        )
    )
    return(patterns)

@pytest.fixture(scope="session")
def get_segment_results(get_rawnav_gdf,get_summary,get_segments,get_patterns):
    # This reassignment may be unnecessary, but was causing grief earlier    
    ranwav_gdf = get_rawnav_gdf 
    summary = get_summary
    segs = get_segments
    patterns = get_patterns

    seg = segs.loc[segs.seg_name_id == "irving_fifteenth_sixteenth_stub"]
    
    index_run_segment_start_end, summary_run_segment = (
        wr.merge_rawnav_segment(
            rawnav_gdf_=ranwav_gdf,
            rawnav_sum_dat_=summary,
            target_=seg,
            patterns_by_seg_=patterns
        )
    )
    return(index_run_segment_start_end,summary_run_segment)


###############################################################################
# Segment Merge Checks
def test_runs_same_in_summary_index(get_segment_results):
    index_run_segment_start_end, summary_run_segment = get_segment_results
    
    index_runs = (
        index_run_segment_start_end
        .filter(items = ['filename','index_run_start'])
        .drop_duplicates()
        .sort_values(['filename','index_run_start'])
        .reset_index(drop = True)
    )

    summary_runs = (
        summary_run_segment
        .filter(items = ['filename','index_run_start'])
        .drop_duplicates()
        .sort_values(['filename','index_run_start'])
        .reset_index(drop = True)
    )
    
    assert(all(index_runs == summary_runs))
    
def test_expect_fail_too_far(get_segment_results):
    index_run_segment_start_end, summary_run_segment = get_segment_results
    
    case_file = "rawnav02833191007.txt"
    case_run = 4175
    
    # this is probably a case where urban canyon effects result in pings appearing far from
    # the segment end. Could be nothing, but we drop these 
    # nearest point to end of segment for run (38.9284, -77.0356)
    # ends up in the church a bit further south
    
    # Not exactly for segment, but can view with the following outside of 
    # this code chunk
    # query_text = 'filename == "rawnav02833191007.txt" & index_run_start == 4175'
    # rawnav_qjump_filt = rawnav_qjump_dat.query(query_text)
    # stop_index_filt = (stop_index.query(query_text))
    # stop_index_filt_line = wr.make_target_rawnav_linestring(stop_index_filt)
    # debug_map = wr.plot_rawnav_trajectory_with_wmata_schedule_stops(rawnav_qjump_filt, stop_index_filt_line)
    # debug_map
     
    index_case = (
        index_run_segment_start_end
        .query('filename == @case_file & index_run_start == @case_run')
    )
    
    fail_result = (
        index_case
        .query('location == "last"')
        .filter(items = ['flag_too_far'])
        .to_numpy()
    )
    
    assert fail_result

def test_explode(get_segments):
    breakpoint()
    segments = get_segments
    seg_irving = segments.loc[segments.seg_name_id == "irving_fifteenth_sixteenth_stub"]
    
    seg_irving_first_last = wr.explode_first_last(seg_irving)
    
    test_first = (    
        (round(seg_irving_first_last.query('location == "first"').geometry.x,2) == round(1301748.294,2))
        & (round(seg_irving_first_last.query('location == "first"').geometry.y,2) == round(459630.749,2))
    )

    assert test_first
    
    test_last = (    
        (round(seg_irving_first_last.query('location == "last"').geometry.x,2) == round(1302192.476353453,2))
        & (round(seg_irving_first_last.query('location == "last"').geometry.y,2) == round(459603.0866485205,2))
    )
    
    assert test_last
    # note that these endpoints were also manually verified on map    
    
def test_expect_nearest_correct(get_segment_results):
    index_run_segment_start_end, summary_run_segment = get_segment_results
    
    case_file = "rawnav02833191007.txt"
    case_run = 4175
    
    # this is probably a case where urban canyon effects result in pings appearing far from
    # the segment end. Could be nothing, but we drop these 
    # nearest point to end of segment for run (38.9284, -77.0356)
    # ends up in the church a bit further south
    
    # Not exactly for segment, but can view with the following outside of 
    # this code chunk
    # query_text = 'filename == "rawnav02833191007.txt" & index_run_start == 4175'
    # rawnav_qjump_filt = rawnav_qjump_dat.query(query_text)
    # stop_index_filt = (stop_index.query(query_text))
    # stop_index_filt_line = wr.make_target_rawnav_linestring(stop_index_filt)
    # debug_map = wr.plot_rawnav_trajectory_with_wmata_schedule_stops(rawnav_qjump_filt, stop_index_filt_line)
    # debug_map
     
    index_case = (
        index_run_segment_start_end
        .query('filename == @case_file & index_run_start == @case_run')
    )
    
    fail_result = (
        index_case
        .query('location == "last"')
        .filter(items = ['flag_too_far'])
        .to_numpy()
    )
    
    assert fail_result
