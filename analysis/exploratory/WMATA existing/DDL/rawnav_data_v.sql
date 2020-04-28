--------------------------------------------------------
--  File created - Tuesday-February-11-2020   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View RAWNAV_DATA_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "PLANAPI"."RAWNAV_DATA_V" ("ID", "BUS_ID", "ROUTE", "PATTERN", "RUN_DATE", "RUN_START_TIME", "RUN_START_HH24", "GPS_READING_TIME", "GPS_READING_DTM", "GPS_READING_TIME_TXT", "LAT_FIXED", "LON_FIXED", "HEADING_DEG", "DOOR_STATE", "VEHICLE_STATE", "ODO_FEET", "TIME_SECONDS", "STOP_WINDOW_DATA", "STOP_WINDOW", "IN_STOP_WINDOW", "NEXT_FEET", "NEXT_SECONDS", "MPH", "MPH_BIN", "MINS_ELAPSED", "GPS_READING_SECS_PAST_MIDNIGHT") AS 
  with data as (
select    a.id, a.bus_id,  substr(route_pattern, 1, 2) route, substr(route_pattern, 3, 2) pattern, the_date run_date, 
    the_time run_start_time, to_char(to_date(the_time, 'HH24:MI:SS'), 'HH24') run_start_hh24,
    to_date(the_time, 'HH24:MI:SS') + time_seconds/60/60/24 gps_reading_time,
    to_date(the_date, 'MM/DD/YY') + (to_date(the_time, 'HH24:MI:SS') - trunc(to_date(the_time, 'HH24:MI:SS'))) + time_seconds/60/60/24 gps_reading_dtm,
    to_char(to_date(the_time, 'HH24:MI:SS') + time_seconds/60/60/24, 'HH24:MI:SS') gps_reading_time_txt,
  b.lat_fixed, b.lon_fixed, b.heading_deg, b.door_state, b.vehicle_state, b.odo_feet, b.time_seconds, b.stop_window_data,

  lead(odo_feet) over (partition by a.id order by time_seconds) next_feet,
  lead(time_seconds) over (partition by a.id order by time_seconds) next_seconds,
  last_value(stop_window_data ignore nulls) over (partition by a.id order by time_seconds rows between unbounded preceding and 0 preceding) stop_window
from rawnav_route_run a, rawnav_gps_reading_v b
where a.id = b.route_run_id
order by a.id, b.time_seconds)
select  c."ID",c."BUS_ID",c."ROUTE",c."PATTERN",c."RUN_DATE",c."RUN_START_TIME",c."RUN_START_HH24",c."GPS_READING_TIME",c."GPS_READING_DTM",c."GPS_READING_TIME_TXT",c."LAT_FIXED",c."LON_FIXED",c."HEADING_DEG",c."DOOR_STATE",c."VEHICLE_STATE",c."ODO_FEET",c."TIME_SECONDS",
  c.stop_window_data, stop_window, decode(substr(stop_window, 1, 1), 'E', 1, 0) in_stop_window,
  c."NEXT_FEET",c."NEXT_SECONDS", ((next_feet - odo_feet) / (next_seconds - time_seconds)) * 0.681818 mph, round(((next_feet - odo_feet) / (next_seconds - time_seconds)) * 0.681818 / 5) * 5 mph_bin,
  (next_seconds - time_seconds) / 60 mins_elapsed,
  (gps_reading_dtm - trunc(gps_reading_dtm)) * 24*60*60  gps_reading_secs_past_midnight
from data c
where time_seconds <> next_seconds
;
