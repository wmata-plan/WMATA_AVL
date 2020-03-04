  CREATE OR REPLACE FORCE EDITIONABLE VIEW "PLANAPI"."RAWNAV_TRIP_ADJ_V" ("ID", "ADJ_FEET", "ADJ_SECS") AS 
  select route_run_id id, max(odo_feet) adj_feet, max(time_seconds) adj_secs
from rawnav_gps_reading
where stop_window_data = 'E01'
group by route_run_id;
