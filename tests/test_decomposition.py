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
import numpy as np
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
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
def get_analysis_route():
    analysis_routes = ['S9']
    return(analysis_routes)
    
@pytest.fixture(scope="session")
def get_seg():
    seg = 'sixteenth_u_stub'
    return(seg)

@pytest.fixture(scope="session")
def get_pattern_stop():
    pattern_stop = (
        pd.DataFrame(
            {'route':['S9'],
             'pattern':[2], 
             'seg_name_id':['sixteenth_u_stub'],
             'stop_id' : [18042]}
        )
    )
    return(pattern_stop)

@pytest.fixture(scope="session")
def get_rawnav(get_analysis_route,get_cwd):

    rawnav_dat = (
        wr.read_cleaned_rawnav(
            analysis_routes_=get_analysis_route,
            path = os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "03_notebook_data",
                "rawnav_data.parquet"
            )
        )
    )
    
    return(rawnav_dat)

@pytest.fixture(scope="session")
def get_segment_summary(get_cwd,get_seg):
    
    segment_summary = (
        pq.read_table(
            source = os.path.join(
                get_cwd,
                "data",
                "00-raw",
                "demo_data",
                "03_notebook_data",
                "segment_summary.parquet"
            ),
            use_pandas_metadata = True)
        .to_pandas()
    )
    
    segment_summary_fil = (
        segment_summary
        .query('~(flag_too_far_any | flag_wrong_order_any | flag_too_long_odom)')
    )
        
    return(segment_summary_fil)

@pytest.fixture(scope="session")
def get_stop_index(get_cwd, get_analysis_route, get_pattern_stop):
    stop_index = (
            pq.read_table(
                source = os.path.join(
                    get_cwd,
                    "data",
                    "00-raw",
                    "demo_data",
                    "03_notebook_data",
                    "stop_index.parquet"
                ),
                columns = ['seg_name_id',
                           'route',
                           'pattern',
                           'stop_id',
                           'filename',
                           'index_run_start',
                           'index_loc',
                           'odom_ft',
                           'sec_past_st',
                           'geo_description']
            )
            .to_pandas()
            .assign(pattern = lambda x: x.pattern.astype('int32')) 
            .rename(columns = {'odom_ft' : 'odom_ft_qj_stop'})
    ) 
    
    stop_index_fil = (
        stop_index
        .merge(get_pattern_stop,
               on = ['route','pattern','stop_id'],
               how = 'inner')   
    )
    return(stop_index_fil)

@pytest.fixture(scope="session")
def get_ff(get_rawnav,get_segment_summary,get_seg):
    segment_ff = (
            wr.decompose_segment_ff(
                get_rawnav,
                get_segment_summary,
                max_fps = 73.3
            )
            .assign(seg_name_id = get_seg)
        )
    
    return(segment_ff)

@pytest.fixture(scope="session")
def get_stop_area_decomp(get_rawnav,get_segment_summary,get_stop_index,get_seg):
    stop_area_decomp = (
        wr.decompose_stop_area(
            get_rawnav,
            get_segment_summary,
            get_stop_index
        )
        .assign(seg_name_id = get_seg)
    )
    
    return(stop_area_decomp)

@pytest.fixture(scope="session")
def get_tt_decomp(get_ff, get_rawnav,get_segment_summary,get_stop_area_decomp):
    segment_ff_val = (
        get_ff
        .loc[0.95]
        .loc["fps_next3"]
    )

    # Run decomposition
    traveltime_decomp = (
        wr.decompose_traveltime(
            get_rawnav,
            get_segment_summary,
            get_stop_area_decomp,
            segment_ff_val
        )
    )
    
    return(traveltime_decomp)

###############################################################################
# Segment Merge Checks
def test_tstop1_and_no_stop(get_stop_area_decomp):
    runs_shouldnt_have_both_these = (
        get_stop_area_decomp
        .loc[
            get_stop_area_decomp
            .groupby(['filename','index_run_start','stop_id'])['stop_area_phase']
            .transform(lambda var: 
                       var.isin(['t_nostopnopax']).any()
                       & var.isin(['t_stop1','t_stop']).any()
            )
        ]
    )
    assert(len(runs_shouldnt_have_both_these ) == 0)
    
def test_flagnostop_tstop2_match(get_tt_decomp):
    runs_shouldnt_have_both_these = (
        get_tt_decomp
        .query('(flag_nostop == False) & (t_stop2 == 0)')
    )
    
    assert(len(runs_shouldnt_have_both_these) == 0)
    
    runs_also_shouldnt_have_both_these = (
        get_tt_decomp
        .query('(flag_nostop == True) & (t_stop2 > 0)')
    )
    
    assert(len(runs_also_shouldnt_have_both_these) == 0)

def test_resum_matches_total(get_tt_decomp):
   
    runs_with_bad_secs_totals = (
        get_tt_decomp
        .assign(
            recalc = lambda x: x.t_ff + x.t_stop + x.t_stop1 + x.t_stop2 + x.t_traffic,
            diff_orig = lambda x: x.recalc - x.t_segment
        )        
        .loc[lambda x: 
             (x.diff_orig.abs().to_numpy() > .5)
        ]
    )

    assert(len(runs_with_bad_secs_totals) == 0)
