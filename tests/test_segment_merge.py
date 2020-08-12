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
    
    return(rawnav)

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
    return(summary)

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
# def get_wmata_schedule_gdf():
#     wmata_schedule_dat = (
#         pd.read_csv(
#             os.path.join(
#                 get_cwd,
#                 "data",
#                 "00-raw",
#                 "demo_data",
#                 "02_notebook_data",
#                 "wmata_schedule_data_q_jump_routes.csv"
#             ),
#             index_col = 0
#         )
#         .reset_index(drop=True)
#     )

#     wmata_schedule_gdf = (
#         gpd.GeoDataFrame(
#             wmata_schedule_dat, 
#             geometry = gpd.points_from_xy(wmata_schedule_dat.stop_lon,wmata_schedule_dat.stop_lat),
#             crs='EPSG:4326')
#         .to_crs(epsg=2248)
#     )
    
#     return(wmata_schedule_gdf)

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
    segs = get_segments
    breakpoint()
    
    # seg = segs.loc[segs.seg_name_id == "irving_fifteenth_sixteenth_stub"])
    
    # index_run_segment_start_end, summary_run_segment = (
    #     wr.merge_rawnav_segment(
    #         rawnav_gdf_=get_rawnav_gdf,
    #         rawnav_sum_dat_=get_summary,
    #         target_=seg,
    #         patterns_by_seg_=get_patterns
    #     )
    # )
    # return(index_run_segment_start_end,summary_run_segment)

# @pytest.fixture(scope="session")
# def get_seg_index():    
#     return(index_run_segment_start_end)

# @pytest.fixture(scope="session")
# def get_seg_summary():    
#     return(summary_run_segment)

###############################################################################
# Segment Merge Checks
def test_me(get_segment_results):
    index_run_segment_start_end, summary_run_segment = get_segment_results
    print('himom')
    
def test_explode(get_segments):
    segments = get_segments
    seg_irving = segments.loc[segments.seg_name_id == "irving_fifteenth_sixteenth_stub"]
    
    seg_irving_first_last = wr.explode_first_last(seg_irving)
    

