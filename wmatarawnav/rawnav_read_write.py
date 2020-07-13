# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:20:18 2020

@author: WylieTimmerman, Apoorba Bibeka
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from . import low_level_fns as ll


def read_cleaned_rawnav(analysis_routes_, path, analysis_days_):
    """
    Parameters
    ----------
    analysis_routes_: list,
        routes for which rawnav data is needed. Should be a subset of following:
        ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
                         'H8','W47']
    path: str,
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
        # Note that forloop on analysis_routes and analysis days filter 
        #  is effectively moot now based on how we iterate,
        #  but leaving this stuff for now to add some degree of flexibility
        filter_parquet = [[('wday', '=', day)] for day in analysis_days_]
        try:
            rawnav_temp_dat = (
                pq.read_table(source=os.path.join(path),
                              filters=filter_parquet,
                              use_pandas_metadata = True)
                .to_pandas())
            
            # NOTE: could selectt fewer columns to save memory/time on read in, but don't expect
            # performance boost to be that great, could be obnoxious
        except Exception as e:
            if str(type(e)) == "<class 'IndexError'>":
                raise ValueError('No data found for given filter conditions')
            else:
                print(e)
                raise
        else:
            # Even after defining the schema on parquet write, we're still seeing some strings 
            # read in as categories rather than as strings. Very odd.
            rawnav_temp_dat.route = rawnav_temp_dat.route.astype('str') 
            rawnav_temp_dat.wday = rawnav_temp_dat.wday.astype('str') 
            # Even though we could store as int, in case the values have NA's, we store as float
            # and then convert to int after the fact. If you happen to run into a problem here, do an adhoc
            # load of the parquet file and then after filtering NA values, convert pattern to 
            # integer.
            rawnav_temp_dat.pattern = rawnav_temp_dat.pattern.astype('int') 

            rawnav_temp_list.append(rawnav_temp_dat)
    rawnav_dat = pd.concat(rawnav_temp_list)
    return rawnav_dat



