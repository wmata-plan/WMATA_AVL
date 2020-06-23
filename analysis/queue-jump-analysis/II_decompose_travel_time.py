# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 03:39:56 2020

@author: WylieTimmerman, Apoorba Bibeka

Note: the sections here are guideposts - this may be better as several scripts
"""

# 0 Housekeeping. Clear variable space
####################################################################################################

# 1 Import Libraries and Set Global Parameters
####################################################################################################


# 1.1 Import Python Libraries
#############################################

import os, sys
import pandas as pd
import geopandas as gpd

# 1.2 Set Global Parameters
############################################

if os.getlogin() == "WylieTimmerman":
    # Working Paths
    path_working = r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(r"C:\OD\OneDrive - Foursquare ITP\Projects\WMATA_AVL")
    path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Queue Jump Analysis"
    # Source data
    # path_source_data = os.path.join(path_sp,r"Client Shared Folder\data\00-raw\102019 sample")
    path_source_data = r"C:\Downloads"
    gtfs_dir = os.path.join(path_sp, r"Client Shared Folder\data\00-raw\wmata-2019-05-18 dl20200205gtfs")
    # Processed data
    path_processed_data = os.path.join(path_sp, r"Client Shared Folder\data\02-processed")
elif os.getlogin() == "abibeka":
    # Working Paths
    path_working = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL"
    os.chdir(os.path.join(path_working))
    sys.path.append(path_working)
    # Source data
    path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data"
    gtfs_dir = os.path.join(path_source_data, "google_transit")
    # Processed data
    path_processed_data = os.path.join(path_source_data, "ProcessedData")
else:
    raise FileNotFoundError("Define the path_working, path_source_data, gtfs_dir, \
                            ZippedFilesloc, and path_processed_data in a new elif block")

# 1.3 Import User-Defined Package
############################################

# WT: I can't get out of my R habits, so defining this one for now!
def tribble(columns, *data):
    return pd.DataFrame(
        data=list(zip(*[iter(data)]*len(columns))),
        columns=columns
    )

# 1.4 Reload Relevant Files 
####################################################################################################

# 1. load segment-pattern-stop crosswalk 
# See earlier other script for example

# 2. load segment-pattern crosswalk (could be loaded separately or summarized from above)

# 3. load segment shapes
# See earlier other script for example

# 2 Add Additional Metrics to Rawnav Data
####################################################################################################

# Apoorba, on reflection, i think there are a few columns we should try to add to the rawnav data
# during the processing phase (I_parse_rawnav.py) that are used repeatedly in the code elsewhere.
# For now, it may be best to simply add these needed fields here, and then move this chunk of code
# into I_parse_rawnav.py later.

# In particular, I think we need the following (calculated in groups by run)
#   SecsPastSt_marg (or some better name): The marginal time between the current rawnav ping and the
#       next one.
#   OdomFt_marg (or some better name):  The marignal distance between the current rawnav ping and the
#       next one.
#   fps_marg (or some better name): The speed between the current rawnav ping and the next one.

# 3 Filter Out Runs that Appear Problematic
####################################################################################################

# Here, i imagine we'll want to use the summary files created in I_III_rawnav_other_merge.py to 
#   identify runs that we should not include because . I think you partially did this with 
#   read_summary_rawnav(), but it may be helpful to punt that to here in case we want to filter
#   out runs that are incomplete in the middle of a segment or the middle of a stop zone.

# 4 Calculate Free Flow Speed
####################################################################################################

# NOTE: we may want to skip writing this function until later and substitute a table of hardcoded
# values in the short-term, just so we can get the overall decomposition approach running.
# Regardless of the choice above, while decomposition will ultimately be produced for individual
# trips, freeflow is calculated as an aggregate value at the segment level. Result will look like
# this:
    
seg_freeflow = tribble(
             ['seg_name_id',              'spd_freeflow_fps'], 
              "georgia_irving",                       44.0,
              "georgia_columbia",                     44.0, 
              "georgia_piney_branch_shrt",            44.0,
              "georgia_piney_branch_long",            44.0,
              "sixteenth_u_shrt",                     44.0,
              "sixteenth_u_long",                     44.0,
              "eleventh_i_new_york",                  44.0,
              "irving_fifteenth_sixteenth",           36.7
  )


# We'll leave open a few options for calculating free flow speed:
    # 1. Hardcode for each segment using posted speed (would need to be weighted somehow if 
    #     a segment had multiple posted speeds, not ideal)
    # 2. Bring in intersection points and calculate speeds between intersections (a little tedious,
    #     may be hard to execute on short gaps between intersections/stops)
    # 3. Take the 95th percentile average speed for the segment (doesn't work well for routes where there are few early
    #     trips or cases where there are many downstream intersections of which most routes catch one)
    # 4. Take a sort of 95th percentile speed by point (current approach)

# wr.calc_seg_freeflow() will return a speed in feet per second calculated from inputs.

#   Note that we want to use speed instead of time. Because each run may have its nearest rawnav 
#   observation just ahead or just before the segment end (and we want to avoid the hassle of 
#   interpolation), we'll calculate a speed here and then use this later to calculate the freeflow
#   travel time in seconds, which we can use for the decomposition. We also want to use feet per
#   second since that's a bit easier to work with downstream.

#   Function Inputs include: 
    # - segment identifier seg_name_id.
    # - segment-pattern crosswalk, identifying what routes run on a segment. Any route running on 
    #   a segment will be used for freeflow, such that an S2's run's freeflow speed may be based on
    #   the S4. This could also just be a list of routes, but we'd ahve to be more careful on the
    #   iteration in that case.
    # - rawnav data
    # - threshold percentile of speed (ala 0.95). 
    
#   Major steps:
    # - filter segment-pattern crosswalk to routes matching seg_name_id. We can't vectorize because
    #   we want to leave open possibility of multiple versions of a segment being defined.
    # - filter to the rawnav pings beginning at point nearest to the beginning point and before the
    #   final point.
    # - For each record in the run, calculate the odometer distance between the current record and the next
    #   run adn the time between the current record and the next run. Then calc the speed in ft per sec.
    #   Likely will group by run and then ungroup here.
    # - calculate Xth percentile speed over all observations.
    
# Again, output of this section is a simple table with one row for every segment and a value for the
#   freeflow speed in feet per second of the segment.

# 4. Do Basic Decomposition of Travel Time by Run
####################################################################################################
# Goal here is to tease out values that we can use to run the currently proposed t_stop2 process
#   or other possible decompositions that may arise later.

# Some complex logic here to implement in a function wr.calc_basic_decomp(). Idea is that we do
#   a basic decomposition here that will be used to extract t_decel_phase and t_accel_phase for 
#   use in the approach outlined by Burak to create a a_acc and a_dec in the methodology workshop
#   Powerpoint. But regardless of how we calculate that, these items are useful. 

# Example result table shown in Powerpoint. Field names can vary to match what we already have! 
#   run_basic_decomp = ...
# Run identifers, including file, date, and run identifier
        # seg_name_id = segment identifier (ala "sixteenth_u") 
        # stop_zone_id = Stop identifier (likely the stop ID) 
        # t_decel_phase = time from start of segment to zero speed (or door open) (used to estimate adec)
        # t_l_inital = time from first door open back to beginning of zero speed, if any
        # t_stop1 = Stop zone door open time defined as first instance of door opening.
        # t_l_addl = time from first door close to acceleration
        # t_accel_phase = time from acceleration to exiting stop zone (used to help estimate aacc
        # t_sz_total = total time in stop zone

# Function inputs include:
    #   - A rawnav file (pulled from Parquet storage, may include many runs and even multiple routes or segments)
    #   - The index_stop_zone_start_end table with one record for the start or stop of each stop zone -- rawnav run -- evaluation segment combination 
    #   - The segment-pattern crosswalk (may be moot given the above)

# Major Steps:
#   - Filter to rawnav records in applicable stop zones (note that because stop zones will not differ
#       even if several variants of a segment are defined, we don't have to do the segment-by-segment
#       iteration here).
#   - Group by run-segment-stop
#   - Calculate a few new fields:
    #   - Calculate a new column door_state_change that increments every time door_state changes, 
    #     ala R's data.table::rleid() function. 
    #     https://www.rdocumentation.org/packages/data.table/versions/1.12.8/topics/rleid
    #   - Calculate a new column SecsPastSt_marg that is the marginal number of seconds past start 
    #       between the current observation and the next observation.
    #       TODO: this could be calculated earlier - this is a recalculation.
    #   - Calculate a new indicator for door_state_stop_zone that is:
    #          - "open" when door_open == "O" and the earliest door_state_change value among door_open records 
    #          - "closed" otherwise 
    #   - See mark up of a rawnav table at this location for more pointers on how the rest of hte
    #       fields above would show up: https://foursquareitp.sharepoint.com/:x:/r/Shared%20Documents/WMATA%20Queue%20Jump%20Analysis/Client%20Shared%20Folder/data/01-interim/run_basic_decomp_example.xlsx?d=w41a23c96379b4187992714719acbc002&csf=1&web=1&e=YfZHo9
#   - Summarize...


# Calculate Stop-level Baseline Accel-Decel Time (input to t_stop2)
####################################################################################################
#TODO : Update accordingly following more discussion with Burak.

# Would require additional filtering of runs meeting or not meeting certain criteria.

# wr.calc_stop_accel_decel_baseline()

# Calculate t_stop2
####################################################################################################

# TODO: write the wr.calc_tstop2s() function


# 3 Travel Time decomposition
####################################################################################################

# Here, we would call a wr.decompose_traveltime() function. I imagine this would take as input

# TODO: write the wr.decompose_traveltime() function
 


