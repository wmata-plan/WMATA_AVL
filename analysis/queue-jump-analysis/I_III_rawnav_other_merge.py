# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 02:22:39 2020

@author: WylieTimmerman, ApoorbaBibeka
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

# 2 Load Relevant Static Files 
####################################################################################################

# 1. load segment-pattern-stop crosswalk 
# - could make this based on the pattern identifiers instead of route and direction, but just 
#   sketching... You'll of course want to replace direction_id with something more rawnav specific
#   and also replace the stop_id.
# - Could also replace with a looked-up Excel file, but leaving as this for now

xwalk_seg_pattern = tribble(
             ['route','direction_id',                   'seg_name_id', 'stop_id'], 
                 "79",            1,               "georgia_columbia",    'XXXX',      
                 "79",            1,      "georgia_piney_branch_long",    'XXXX',
                 "70",            1,                 "georgia_irving",    'XXXX',
                 "70",            1,                 "georgia_irving",    'XXXX',
                 "70",            1,      "georgia_piney_branch_shrt",    'XXXX',
                 "S1",            0,               "sixteenth_u_shrt",    'XXXX',
                 "S2",            0,               "sixteenth_u_shrt",    'XXXX',
                 "S4",            0,               "sixteenth_u_shrt",    'XXXX',
                 "S9",            0,               "sixteenth_u_long",    'XXXX',
                 "64",            0,            "eleventh_i_new_york",    'XXXX',
                 "G8",            0,            "eleventh_i_new_york",    'XXXX',
                "D32",            0,     "irving_fifteenth_sixteenth",    'XXXX',
                 "H1",            0,     "irving_fifteenth_sixteenth",    'XXXX',
                 "H2",            0,     "irving_fifteenth_sixteenth",    'XXXX',
                 "H3",            0,     "irving_fifteenth_sixteenth",    'XXXX',
                 "H4",            0,     "irving_fifteenth_sixteenth",    'XXXX',
                 "H8",            0,     "irving_fifteenth_sixteenth",    'XXXX',
                "W47",            0,     "irving_fifteenth_sixteenth",    'XXXX'
  )

# 2. load segment-pattern crosswalk (could be loaded separately or summarized from above)

# 3. load shapes
# WT: I leave it up to you how you want to manage this -- for now, i have the file 
#   available for download as a geojson here: 
#   

# Segments
# Note unique identifier seg_name_id
# Note that these are not yet updated to reflect the extension of the 11th street segment 
# further south to give the stop more breathing room.
seguments = gpd.read_file(
    os.path.join(path_processed_data,"segments.geojson"))

# Intersections
# WT: TBD -- Will do some of this work in ArCGIS first


# 2 Merge Additional Geometry
####################################################################################################

# First, we'd run additional 'merge' style functions that take as input rawnav data (possibly
#   filtered to a route or day) and other geometry and then return data frames returning the 
#   index of points nearest to these geometries, beginning with segments.

# NOTE: we may want to combine this work with the I_II_rawnav_wmata_schedule_merge.py script
# since it's performing a pretty similar function. Up to you!

# 2.1 Rawnav-Segment ########################

# 2.1.1. Identify points at the start and end of each segment for each run 
    # Similar approach (if not the exact same!) as the approach used for the GTFS merge. 
    # Iterate over the runs using function called something like wr.parent_merge_rawnav_segment(). 
    #   Take care to ensure each run can be associated with several segments. Take care to ensure
    #   that if multiple variants of a segment were defined during testing (ala a version that starts
    #   upstream 75 feet from previous intersection vs. 150 feet from previous intersection) we 
    #   could still make the function work.
    # Return two dataframes
        # 1. index of segment start-end and nearest rawnav point for each run  (index_segment_start_end)
            #   results in two rows per run per applicable segment, one observation for start, one for end
        # 2. segment-run summary table (summary_segment)
            #    one row per run per applicable segment, information on start and end observation and time, dist., etc.

# 2.1.2 After the fact checks on rawnav-segment merge:
    # I imagine we should do some double-checking that the results are about what we want for each run:
    #  - Are the points nearest to the end of the segment within ~ X feet?
    #  - Are the nearest points in order, such that the segment start point has a lower index value than the
    #    segment end point (checks that the segment was drawn in the right direction and that any
    #    future bidirectional segment is actually drawn once for every segment)
    #  -  Are rawnav points continuously within a buffer of ~Y feet around a segment?
    # At this point, one could further filter the rawnav_run_iteration_frame created in #2 to those 
    #   meeting certain criteria for quality (e.g., average speed not insanely high or low, total
    #   travel time or travel distance not crazy).
             
# 2.2 Rawnav-Stop Zone Merge ########################

# Essentially, repeat the above steps or those for the schedule merge, but for stop zones. Stop
#   zones here defined as a number of feet upstream and a number of feet downstream from a QJ stop.
#   May want to change language, as I think Burak wasn't keen on it. 

# Likely would build on 
#   nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat to avoid rework. Because a stop
#   may be in multiple segments and we may eventually want to define multiple segments for a given
#   route during testing, we can't add a new column in the rawnav data that says 'stop zone' or 
#   anything. 

# A function wr.parent_merge_rawnav_stop_zone() would include an
#   argument for the number of feet upstream and downstream from a stop.

# An input would be a segment-pattern-stop crosswalk. Some segments will have 
#   several stops (Columbia/Irving), some stops will have multiple routes, etc. Note that we 
#   wouldn't want to bother with calculating these zones for every stop, but only QJ stops in our segments.
#   Eventually we might want to write the function such that it incorporates first or last stops,
#   but I doubt it. Another input would be the rawnav ping nearest to each stop you made earlier.
#   The 150 feet up and 150 down would be measured from this point.
    
# Similar to above, we'd then return two dataframes
    # 1. index of stop zone start-end and nearest rawnav point for each run (index_stop_zone_start_end)
        #   results in two rows per run per applicable stop zone, one observation for start, one for end
    # 2. segment-run summary table (summary_stop_zone)
        #    one row per run per applicable segment, information on start and end observation and time, dist., etc.

# Segments are drawn to end 300 feet up from next stop, but we may at some point want to chekc that
#   the next downstream stop's zone doesn't extend back into our evaluation segment.


# 2.3 Rawnav-Intersection Merge ######################## 

# To follow, time permitting.