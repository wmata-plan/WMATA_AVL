  CREATE OR REPLACE FORCE EDITIONABLE VIEW "PLANAPI"."RAWNAV_GPS_READING_V" ("ID", "LAT_FIXED", "LON_FIXED", "HEADING_DEG", "DOOR_STATE", "VEHICLE_STATE", "ODO_FEET", "TIME_SECONDS", "SAT_COUNT", "STOP_WINDOW_DATA", "ROUTE_RUN_ID") AS 
  select a.id, lat_fixed, lon_fixed, heading_deg, door_state, vehicle_state, odo_feet - adj_feet odo_feet, time_seconds - adj_secs time_seconds, sat_count, stop_window_data, route_run_id
from rawnav_gps_reading a join rawnav_trip_adj_v b on a.route_run_id = b.id
where time_seconds - adj_secs >= 0;
