"""
Created on Tue Aug 11 16:09:09 2020
@author: abibeka
"""
import pandas as pd
import os
import geopandas as gpd
import sys
import pyarrow as pa
import pyarrow.parquet as pq
path_working = (r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc"
                r"\Documents\Github\WMATA_AVL")
sys.path.append(path_working)
import wmatarawnav as wr  # noqa E402
from helper_function_field_validation \
    import quick_and_dirty_schedule_qjump_mapping  # noqa E402
path_source_data = (
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents"
    r"\WMATA-AVL\Data\field_rawnav_dat")
path_processed_data = os.path.join(path_source_data, "processed_data")
path_processed_route_data = os.path.join(path_processed_data, "RouteData")
path_field_dir = (
    r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents'
    r'\WMATA-AVL\Data\field_rawnav_dat')
path_debug_dir = os.path.join(path_processed_data, "debug")
path_wmata_schedule_data = (r"C:\Users\abibeka\OneDrive - "
                            r"Kittelson & Associates, Inc\Documents"
                            r"\WMATA-AVL\Data\wmata_schedule_data"
                            r"\Schedule_082719-201718.mdb")
path_share_data_with_Wylie = (r"C:\Users\abibeka\OneDrive - Kittelson &"
                              r" Associates, Inc\Documents\Github\WMATA_AVL"
                              r"\analysis\exploratory"
                              r"\03_Compare_rawnav_to_field_dir")
# Globals
# Restrict number of zip files to parse to this number for testing.
# For all cases, use None
restrict_n = None
analysis_routes = [
    'S1', 'S2', 'S4', 'S9', '70', '79', '64',
    'G8', 'D32', 'H1', 'H2', 'H3', 'H4',
    'H8', 'W47']
analysis_days = [
    'Monday', 'Tuesday', 'Wednesday', 'Thursday',
    'Friday', 'Saturday', 'Sunday']
# EPSG code for WMATA-area work
wmata_crs = 2248

# 1. Read 126 trip rawnav (observed in field) and rawnav field summary. 
# -----------------------------------------------------------------------------
# 1.1 126 trip rawnav data
rawnav_qjump_dat = pd.read_csv(os.path.join(path_debug_dir, 
                                      "rawnav_data_126_field_trips.csv"))
# 1.2 Summary--field data
path_validation_df = os.path.join(path_processed_data, "field_rawnav_dat.csv")
rawnav_field_summary_dat = (pd.read_csv(path_validation_df, index_col=0)
                            .query("has_data_from_rawnav_unprocessed == 1"))
# 2. Read travel time decomposition data.
# -----------------------------------------------------------------------------
path_stop_rawnav = os.path.join(path_processed_data, "rawnav_stop_areas")
rawnav_tt_decomp = pd.read_csv(os.path.join(path_stop_rawnav,
                                            'traveltime_decomp.csv'),
                               index_col=0)
rawnav_tt_decomp = (
    rawnav_tt_decomp
    .drop_duplicates(["filename", "index_run_start"])
)
# 3. Check if some the trips in rawnav_field_summary_dat where no processed
# data is there are present in traveltime_decomp.
# -----------------------------------------------------------------------------
check = (rawnav_field_summary_dat
 .merge(rawnav_tt_decomp,
        on=["filename", "index_run_start", "seg_name_id"],
        how='inner')
)
check.has_data_from_rawnav_processed.min()

# Check if some the trips in rawnav_field_summary_dat 
# where no processed data is there are present
#in traveltime_decomp---this don't look at the seg_name_id

check2 = (rawnav_field_summary_dat
 .merge(rawnav_tt_decomp,
        on=["filename", "index_run_start"],
        how='inner')
)
check2.has_data_from_rawnav_processed.min()
# Weird! Found 12 trips here. What happened to these before?

# 4. Check if data is being deleted during segment merge function
# -----------------------------------------------------------------------------

segment_summary = (
    pq.read_table(source = os.path.join(path_processed_data,"segment_summary.parquet"),
                  use_pandas_metadata = True)
    .to_pandas()
)
segment_summary = (segment_summary[
    ~segment_summary.duplicated(['filename', 'index_run_start'],
                                keep='last')
    ])

rawnav_field_summary_dat_test = (rawnav_field_summary_dat[['filename',
                                          'index_run_start',
                                          'field_qjump_loc',
                                          'metrobus_route_field',
                                          'pattern']]
                                     .rename(columns={'pattern':
                                                      'pattern_field'}))

rawnav_field_summary_dat_test_seg = \
    rawnav_field_summary_dat_test.merge(segment_summary, how="left")

rawnav_field_summary_dat_test_seg.to_csv(
    os.path.join(path_share_data_with_Wylie,"test_rawanv_segment_merge.csv"))
# TODO: This the problem!
# 5. Check if data is being deleted during stop merge function
# -----------------------------------------------------------------------------
stop_index = (
    pq.read_table(source=os.path.join(path_processed_data,"stop_index.parquet"),
                    columns = ['seg_name_id',
                                'route',
                                'pattern',
                                'stop_id',
                                'filename',
                                'index_run_start',
                                'index_loc',
                                'odom_ft',
                                'sec_past_st',
                                'geo_description'],
                  use_pandas_metadata = True)
    .to_pandas()
    .assign(pattern = lambda x: x.pattern.astype('int32')) #  pattern is string not int? # TODO: fix
    .rename(columns = {'odom_ft' : 'odom_ft_stop'})
) 

qjump_stop_file = os.path.join(path_debug_dir, 
                                 "qjump_stop_wo_deletion.csv")
xwalk_seg_pattern_stop_by_run = (
    pd.read_csv(qjump_stop_file, index_col=0)
    .filter(items=["filename",  "index_run_start", "field_qjump_loc",
                   "seg_name_id", "stop_id", "geo_description"])
    )


rawnav_field_summary_dat_test_stop = \
    xwalk_seg_pattern_stop_by_run.merge(stop_index,on=[
    "filename", "index_run_start", "stop_id"], how="left")

rawnav_field_summary_dat_test_stop.to_csv(
    os.path.join(path_share_data_with_Wylie,"test_rawanv_stop_merge.csv"))