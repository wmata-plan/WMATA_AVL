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




# 1.2 Set Global Parameters
############################################




# 1.3 Import User-Defined Package
############################################



# 2 Load Relevant Static Files 
####################################################################################################

# 1. load segment-pattern-stop crosswalk 

# 1. load segment-pattern crosswalk (could be loaded separately or summarized from above)

# 2. load segment shapes

# 2 Merge Additional Geometry
####################################################################################################

# First, we'd run additional 'merge' style functions that take as input rawnav data (possibly
#   filtered to a route or day) and other geometry and then return data frames returning the 
#   index of points nearest to these geometries, beginning with segments.

# Note : could break this into a separate script. 
    # GTFS merge is in a separate script and this is a similar process, so perhaps that makes sense
    # Would it make sense to combine any of this with GTFS merge, since we have to convert rawnav
    #   points to point geometry at that point too? Probably better to keep them untangled, but 
    #   maybe we need to create a spatially aware rawnav dataset outside of the context of these 
    #   individual functions (i.e. during the original processing piece?)

# 2.1 Rawnav-Segment ########################

# 2.1.1. Identify points at the start and end of each segment for each run 
    # Similar approach (if not the exact same!) as the approach used for the GTFS merge. 
    # Iterate over the runs using function like a wr.parent_merge_rawnav_segment()
    #   Take care to ensure each run can be associated with several segments. Take care to ensure
    #   that if multiple variants of a segment were defined during testing (ala a version that starts
    #   upstream 75 feet from previous intersection vs. 150 feet from previous intersection) we 
    #   could still make the function work.
    # Return two dataframes
        # 1. index of segment start-end and nearest rawnav point for each run 
            #   results in two rows per run per applicable segment, one observation for start, one for end
        # 2. segment-run summary table
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

# Essentially, repeat the above steps or those for the schedule merge, but for stop zones. 
# Likely would build on 
#   nearest_rawnav_point_to_wmata_schedule_correct_stop_order_dat to avoid rework. Because a stop
#   may be in multiple segments and we may eventually want to define multiple segments for a given
#   route during testing, we can't add a new column in the rawnav data that says 'stop zone' or 
#   anything. 

# For each stop, we want to identify a stop zone around the stop defined as a number of feet upstream
#   and a number of feet downstream. A function wr.parent_merge_rawnav_stop_zone() would include an
#   argument for the number of feet upstream and downstream from a stop.

# WT: personally, i want to think harder about how it affects the analysis if passenger boarding/
#   alighting happens much farther away from the stop point, either upstream or downstream.

# An input would be a segment-pattern-stop crosswalk. Some segments will have 
#   several stops (Columbia/Irving), some stops will have multiple routes, etc. Note that we 
#   wouldn't want to bother with calculating these zones for every stop, but only QJ stops in our segments.
#   Eventually we might want to write the function such that it incorporates first or last stops,
#   but I doubt it.
    
# Similar to above, we'd then return two dataframes
    # 1. index of stop zone start-end and nearest rawnav point for each run 
        #   results in two rows per run per applicable stop zone, one observation for start, one for end
    # 2. segment-run summary table
        #    one row per run per applicable segment, information on start and end observation and time, dist., etc.

# Segments are drawn to end 300 feet up from next stop, but we may at some point want to chekc that
#   the next downstream stop's zone doesn't extend back into our evaluation segment.

# 3 Calculate Free Flow Speed
####################################################################################################

# Though decomposition will ultimately be produced for individual trips, freeflow is calculated as 
# an aggregate value at the segment level. Alternatively, we might just substitute a hardcoded value
# based on the posted speed, so for now, we don't want to incorporate this too too tightly into
# other code. we will almost certainly need to allow WMATA to incorporate hardcoded values for 
# routes that don't have many early, late trips. Will also need to make sure this function runs 
# at the segment level, but the catch is that S9 and 79 trips don't run early enough to get a
# freeflow speed. But if you try to use speeds from another route (ala S2, S4 or 70), they have too 
# many stops to be useful for calculating freeflow for a limited stop route.

# important to filter out any runs that are unrealistically fast or slow or have missing data
# before running this step.

# NOTE: we may want to skip writing this function until later and substitute a table of hardcoded
# values in the short-term.

# wr.calc_seg_freeflow() will return a speed in mph calculated from inputs.
#   idea is that this will be run once for each segment in iteration to create a table 
#   with one record for every segment and the resulting freeflow speed. This simple table
#   could be overwritten with values based on the posted speed limit.

#   Note that we want to use speed instead of time. Because each run may have its nearest rawnav 
#   observation just ahead or just before the segment end (and we want to avoid the hassle of 
#   interpolation), we'll calculate a speed here and then use this later to calculate the freeflow
#   travel time in seconds, which we can use for the decomposition

#   Function Inputs include: 
    # - segment identifier seg_name_id.
    # - segment-pattern crosswalk, identifying what routes run on a segment. Any route running on 
    #   a segment will be used for freeflow, such that an S2's run's freeflow speed may be based on
    #   the S4. This could also just be a list of routes, but we'd ahve to be more careful on the
    #   iteration in that case.
    # - rawnav segment summary produced in Rawnav-Segment Merge (note: would be for multiple routes)
    # - threshold percentile of travel time (ala 0.05). We may even want to consider the minimum to avoid
    #   negative values in downstream calcs. Used to define freeflow.
    
#   Major steps:
    # - filter segment-pattern crosswalk to routes matching seg_name_id
    # - filter rawnav-segment merge data to those in the segment-pattern crosswalk
    # - calculate Xth percentile low travel time. Potentially
    #   return other values (e.g., mean, median) in other columns for the sake of reporting.
    
# Again, output of this section is a simple table with one row for every segment and a value for the
#   freeflow speed in MPH of the segment.

# Currently the merged summary tables are separate for each route and DOW. They would need to be 
#   combined into a single table before this step.

# 4 Calculate t_stop1 (door open boarding time)
####################################################################################################

# t_stop1 is calculated at the run-segment-stop level. 

# Return a table of door open time within a stop zone for every run-segment-stop combination. This 
#   table will be taken as an input to the final travel time decomposition functions and (potentially)
#   used to help calculate the baseline accel/decel profile used for t_stop2. Therefore, because of 
#   the potential for multiple dependencies, we calculate this outside of a parent travel time 
#   decomposition function. The simplicity of the calculation also lends itself well to vectorization
#   over a very large rawnav dataset or if t_stop1 style boarding time is needed for a large number
#   of stops.

# wr.calc_stop_t_stop1() would take as input:
#   - A rawnav file (pulled from Parquet storage, may include many runs and even multiple routes or segments)
#   - A table with one record for the start or stop of each stop zone -- rawnav run -- evaluation segment combination
#   - The segment-pattern-stop crosswalk 

# Major steps:
#   - Filter to rawnav records in applicable stop zones
#   - Group by run-segment-stop
#   - Calculate a new column door_state_change that increments every time door_state changes, 
#     ala R's data.table::rleid() function. 
#     https://www.rdocumentation.org/packages/data.table/versions/1.12.8/topics/rleid
#   - Calculate a new column SecsPastSt_lead that is the value of seconds past start for the NEXT
#     observation. 
#   - Filter again to the first set of door open values returned in door_state_change.
#           i.e., door_open == "O" and door_state_change == "2"
#   - Summarize, picking the earliest (or first) SecPastSt in the group and the latest (or last)
#       SecsPastSt_lead. This way, the time after the last door open value and the next door closed
#       time is accounted for.
#   - Calculate the difference between the two above values.

# Again, it would return a table with a record for every run-segment-stop combination and a value
#   for t_stop1. Again, we may want to future proof by allowing for cases where we test out multiple
#   segment lengths for the same queue-jump area.

# After the fact checks:
#   - we should check whether runs have door open time outside of the stop zone. May be worth flagging
#     these runs should be filtered out before further analysis takes place, for instance.
#   - flag whether a door open state was observed at the start of the stop zone. In such cases,
#     we might have missed door open time in a preceeding observation (unlikely that door opened
#     and bus moved, but rawnav pings could do weird things if position recalibrates after a short delay)

# Calculate Stop-level Baseline Accel-Decel Time (input to t_stop2)
####################################################################################################
#TODO :rephrase function to be broader, possibly incorporate into the above to avoid rework.

# How to calculate t_stop2 is still in the air, but several of the considered approaches require the
#   use of a sort of expected accel/decel time around a stop. This would be based on individual 
#   observations or hardcoded through the use of TCQSM methods. Though the details are still being 
#   worked out, we'll want to return a table that includes the accel/decel time around each stop.
#   (To accomodate the fact that rawnav pings will occur just inside/outside of stop area boundaries
#   we may want to return a sort of speed isntead of travel time here, but will deal with that later).  
#   Because each segment could include multiple stops (see Columbia/Irving), this must be done at 
#   the stop level. 

# wr.calc_stop_accel_decel_baseline() would return a value for the stop-zone door-closed travel time based 
#   on the following inputs:
# 
#   Function Inputs include: 
    #   - Rawnav data
    #   - t_stop1 results by run-segment-stop
    #   - A table with one record for the start or stop of each stop zone -- rawnav run -- evaluation segment combination
    #   - segment-pattern-stop crosswalk, identifying what routes run on a segment. Any route running on 
    #     a segment will be used for freeflow, such that an S2's run's freeflow speed may be based on
    #     the S4. This could also just be a list of routes, but we'd ahve to be more careful on the
    #     iteration in that case.

#   Major steps include:
    #   - Filter to rawnav records in applicable stop zones
    #   - Group by run-segment-stop
    #   - Summarize the first/earliest timestamp and the last/latest timestamp of rawnav records in the stop zones
    #   - Calculate the time interval from the above values. This is the totla time in the stop zone
    #   - Join the t_stop1 values calculated earlier and subtract form the above. This is the total
    #       door closed time in the stop zone (including repeated door openings in the closed door time).
    #   - TODO: finish documenting this step

# Returns a table with one record for every run-segment-stop combination and a value for the 
#   total stop zone time and total door closed time 

# again, the output of this section is a simple table with one row for every segment-stop-combination
#   and a value for the door-closed accel/decel time in that stop segment. this will allow for the
#   use of both a rawnav-derived approach and a hardcoded TCQSM-style approach. 

#NOTE: we may want to write this function later and start with a set of hardcoded/arbitrary values
#   just to make sure the data is in the right 'shape' before we start to write a bunch of code. It's 
#   also going to be more complicated to incorporate t_stop1 results and the results of multiple
#   routes on the same segment, so we'd better make sure our other approaches are working before
#   going in depth on this.

# 3 Travel Time decomposition
####################################################################################################

# Setup iteration over each of the run-segment records used for iteration 
    # Here, we would call a wr.decompose_traveltime() function. I imagine this would take as input

# TODO: write the wr.decompose_traveltime() function
 


