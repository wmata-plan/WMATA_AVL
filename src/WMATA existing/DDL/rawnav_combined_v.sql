--------------------------------------------------------
--  File created - Tuesday-February-11-2020   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View RAWNAV_COMBINED_V
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "PLANAPI"."RAWNAV_COMBINED_V" ("ID", "ROUTE_RUN_ID", "THE_DATE", "THE_TIME", "ROUTE_PATTERN", "BUS_ID", "LAT_ORIG", "LON_ORIG", "LAT_FIXED", "LON_FIXED", "HEADING_DEG", "DOOR_STATE", "VEHICLE_STATE", "ODO_FEET", "PREV_ODO_FEET", "DISTANCE_ELAPSED", "MIN_ODO_FEET", "TRIP_DISTANCE_ELAPSED", "TIME_SECONDS", "PREV_TIME_SECONDS", "TIME_ELAPSED", "SPEED1", "SPEED2", "SPEED3", "SAT_COUNT", "STOP_WINDOW_DATA") AS 
  select r.id
    , g.route_run_id 
    , the_date
    , the_time
    , route_pattern
    , bus_id
    , lat_orig
    , lon_orig
    , lat_fixed
    , lon_fixed
    , heading_deg
    , door_state
    , vehicle_state
    , odo_feet
    , lag(odo_feet) over (partition by the_date, the_time, bus_id order by g.id) as prev_odo_feet
    , odo_feet - lag(odo_feet) over (partition by the_date, the_time, bus_id order by g.id) as distance_elapsed
    , min_odo_feet
    , odo_feet - min_odo_feet as trip_distance_elapsed
    , time_seconds
    , lag(time_seconds) over (partition by the_date, the_time, bus_id order by g.id) as prev_time_seconds
    , time_seconds - lag(time_seconds) over (partition by the_date, the_time, bus_id order by g.id) as time_elapsed
    , round(case when (time_seconds - lag(time_seconds) over (partition by the_date, the_time, bus_id order by g.id)) > 0
            then (odo_feet - lag(odo_feet) over (partition by the_date, the_time, bus_id order by g.id))/(time_seconds - lag(time_seconds) over (partition by the_date, the_time, bus_id order by g.id)) *3600/5280 
            else 0 end,2) as speed1
    , round(case when (time_seconds - lag(time_seconds,2) over (partition by the_date, the_time, bus_id order by g.id)) > 0
            then (odo_feet - lag(odo_feet,2) over (partition by the_date, the_time, bus_id order by g.id))/(time_seconds - lag(time_seconds,2) over (partition by the_date, the_time, bus_id order by g.id)) *3600/5280 
            else 0 end,2) as speed2
    , round(case when (time_seconds - lag(time_seconds,3) over (partition by the_date, the_time, bus_id order by g.id)) > 0
            then (odo_feet - lag(odo_feet,3) over (partition by the_date, the_time, bus_id order by g.id))/(time_seconds - lag(time_seconds,3) over (partition by the_date, the_time, bus_id order by g.id)) *3600/5280 
            else 0 end,2) as speed3
    , sat_count
    , stop_window_data
from rawnav_gps_reading g
inner join rawnav_route_run r
    on g.route_run_id = r.id
inner join
    (select route_run_id
        , min(odo_feet) as min_odo_feet
    from rawnav_gps_reading
    group by route_run_id
    ) m
    on m.route_run_id = g.route_run_id
;
