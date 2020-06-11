# -*- coding: utf-8 -*-
"""
Created on Wed Jun 10 03:48:45 2020

@author: WylieTimmerman
"""
# NOTE: To run tests, open terminal, activate environment, change directory
# to the repository, and then run 
# pytests tests

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

ZippedFilesDirParent = os.path.join("data/00-raw/demo_data")

FileUniverse = glob.glob(os.path.join(ZippedFilesDirParent,'rawnav*.zip'))

rawnav_inventory = wr.find_rawnav_routes(FileUniverse, nmax = None, quiet = True)

AnalysisRoutes = ['U6']
rawnav_inventory_filtered = rawnav_inventory[rawnav_inventory.groupby('filename')['route'].transform(lambda x: x.isin(AnalysisRoutes).any())]
rawnav_inventory_filtered = rawnav_inventory_filtered.astype({"line_num": 'int'})
rawnav_inv_filt_first = rawnav_inventory_filtered.groupby(['fullpath','filename']).line_num.min().reset_index()
rawnav_inventory_filtered.head()

RouteRawTagDict = {}

for index, row in rawnav_inv_filt_first.iterrows():
    tagInfo_LineNo = rawnav_inventory_filtered[rawnav_inventory_filtered['filename'] == row['filename']]
    Refrence = min(tagInfo_LineNo.line_num)
    tagInfo_LineNo.loc[:,"NewLineNo"] = tagInfo_LineNo.line_num - Refrence-1
    # FileID gets messy; string to number conversion loose the initial zeros. "filename" is easier to deal with.
    temp = wr.load_rawnav_data(ZipFolderPath = row['fullpath'], skiprows = row['line_num'])
    RouteRawTagDict[row['filename']] = {'RawData':temp,'tagLineInfo':tagInfo_LineNo}

RawnavDataDict = {}
SummaryDataDict = {}

for key, datadict in RouteRawTagDict.items():
    Temp = wr.clean_rawnav_data(datadict, key)
    RawnavDataDict[key] = Temp['rawnavdata']
    SummaryDataDict[key] = Temp['SummaryData']

###############################################################################
# Readin Checks

def test_expect_certain_flags_1():
    # Expect to get certain set of tags from a file
    # We manually reviewed a rawnav file for tags. The inventory return
    # should match this list
    found_tags = rawnav_inventory.loc[rawnav_inventory['file_id'] == '00008191007','taglist'].tolist()
    
    expected_tags = json.loads('["9,PO04726,8,10/06/19,05:15:24,36476,05280", "608,   U601,8,10/06/19,05:36:41,36476,05280", "1819,   U602,8,10/06/19,06:00:10,36476,05280", "2489,   U601,8,10/06/19,06:21:00,36476,05280", "3645,   U602,8,10/06/19,06:48:51,36476,05280", "4323,   U601,8,10/06/19,07:09:00,36476,05280", "5442,   U602,8,10/06/19,07:34:14,36476,05280", "6213,   U601,8,10/06/19,07:57:00,36476,05280", "7364,   U602,8,10/06/19,08:23:59,36476,05280", "8076,   U601,8,10/06/19,08:45:00,36476,05280", "9262,   U602,8,10/06/19,09:13:59,36476,05280", "9982,PI04326,8,10/06/19,09:30:15,36476,05280", "11020,   6403,8,10/06/19,10:29:28,36476,05280", "12343,   6402,8,10/06/19,11:19:01,36919,05280", "13577,   6403,8,10/06/19,11:57:00,36919,05280", "14944,   6402,8,10/06/19,12:49:02,36919,05280", "16133,   6403,8,10/06/19,13:29:59,36919,05280", "17467,   6402,8,10/06/19,14:19:02,36919,05280", "17547,   6402,8,10/06/19,14:23:33,36919,05280", "18308,   6402,8,10/06/19,14:45:13,36919,05280", "18682,PI04313,8,10/06/19,14:52:55,36919,05280", "19289,  TTT06,8,10/06/19,15:17:25,36919,05280", "19721,PO04748,8,10/06/19,15:43:01,36919,05280", "19729,PO04748,8,10/06/19,15:55:27,36919,05280", "19756,   H401,8,10/06/19,16:07:02,36919,05280", "21382,   H403,8,10/06/19,17:01:03,36919,05280", "23022,PI04347,8,10/06/19,17:54:29,36919,05280", "23325,PO04728,8,10/06/19,18:30:50,36919,05280", "24021,   9605,8,10/06/19,18:47:00,36919,05280", "26631,   9606,8,10/06/19,20:21:16,36919,05280", "29295,PI04323,8,10/06/19,21:30:32,36919,05280"]')
    
    assert found_tags == expected_tags

def test_expect_first_row():
    # expect that first lines are what you would expect
    # Note that we drop the last column, as the NaN's there make for problems
    # in comparison 
    found_first = RouteRawTagDict.get("rawnav00008191007.txt").get("RawData").head(1).drop(['TripEndTime'], axis = 1).values.tolist()
    
    expected_first = json.loads('[[0, "38.921298", -76.969803, "312", "C", "S", 0.0, 0.0, 17.0, "   ", 9.0, 38.921298, -76.969803]]')
    
    assert found_first == expected_first

def test_expect_nlines():
    # expect that the number of lines is expected after removing certain rows
    df = RawnavDataDict.get("rawnav00008191007.txt").head(1)
    found_start_end = df[['IndexTripStartInCleanData','IndexTripEndInCleanData']].values.tolist()
    
    # this one indeed ends on 597
    # The value might be confusing though - this is based on index after 
    # read in, not based on index after tags removed
    expected_start_end = [[0.0, 597.0]]
    
    assert found_start_end == expected_start_end

def test_summary_match():
   # Check that the summary file is consistent with the underlying rawnav data
   # for the second trip in rawnav00008191007, starts at python index 599
   # after removing initial lines. has 0 for secs and odom ft
   # this trip ends at python index 1807 after initial lines removed
   # this should match the summary
   test_summary = SummaryDataDict.get("rawnav00008191007.txt")
   
   found_summary_vals = test_summary[(test_summary.taglist == "608,   U601,8,10/06/19,05:36:41,36476,05280")][['SecEnd', 'OdomFtEnd','TripDurFromSec']].values.tolist()
   
   # TODO: update test to use processed rawnav data instaed of referring back
   # to the text file
   # test_rawnav = RawnavDataDict.get("rawnav00008191007.txt")
   
   # found_rawnav_vals = test_rawnav[(test_rawnav.)]

   expect_summary_vals = [[1409, 28214, 1409]]
   # also manually checked others here, but only wrote tests for this.
   
   assert found_summary_vals == expect_summary_vals
   
# expect that the apc tags are added as expected
    # The APC data seems unusable so will hold off on this
    # Anecdotally, seems to match

# Expect no duplicate results after certain joins occur

###############################################################################
# Data Quality Checks

# Expect that we get a rawnav record to look like a busstate line

# Expect that speeds don't exceed __ mph

# expect that odometer and seconds are monotonically increasing 

# trips that begin with odometer or secs after 0?
