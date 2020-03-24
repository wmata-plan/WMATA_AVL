#*******************************************************************************
#PROJECT:       WMATA AVL
#DATE CREATED:  Tue Mar 17 04:33:59 2020
#TITLE:         Data Read in
#AUTHOR:        Wylie Timmerman (wtimmerman@foursquareitp.com)
#BACKGROUND:    Loads in some GTFS data 
#*******************************************************************************

#
#Read and briefly clean
rawnav_raw <- 
  readxl::read_excel(
    path = file.path(datadir,
                     "Rawnav project sample data",
                     "Rawnav sample data 2019 05 01.xlsx"),
    sheet = 1
  ) %>%
  janitor::clean_names()

rawnav_interim <- 
  rawnav_raw %>%
  st_as_sf(., 
           coords = c("lon_fixed", "lat_fixed"),
           crs = 4326L, #WGS84
           agr = "constant",
           remove = FALSE) %>%
  #cleaning up a few fields into posixct
  mutate(run_start_time_pxct = lubridate::mdy_hms(paste0(run_date," ",run_start_time)),
         gps_reading_time_pxct = lubridate::dmy_hms(gps_reading_time),
         gps_reading_dtm_pxct = lubridate::dmy_hms(gps_reading_dtm) #what is this field, exactly?
  ) %>%
  group_by(id,bus_id) %>%
  arrange(gps_reading_secs_past_midnight,.by_group = TRUE) %>% #hope this is reasonable!
  mutate(rowno = row_number()) %>%
  #Some additional calcs we'll reuse later
  #Coudl assume defaults are 0, but a bit cleaner to use NA and show we don't know
  mutate(prev_odo_feet_marginal = odo_feet - lag(odo_feet, default = NA),
         prev_seconds_marginal = time_seconds - lag(time_seconds, default = NA),
         prev_mph = (prev_odo_feet_marginal / 5280) / (prev_seconds_marginal / 3600),
         prev_mph = ifelse(is.nan(prev_mph),NA,prev_mph),
         prev_accel =  (prev_mph - lag(prev_mph, default = NA)) / prev_seconds_marginal - lag(prev_seconds_marginal, default = NA),
         prev_accel = ifelse(is.nan(prev_accel),NA,prev_accel),
         next_mph = lead(prev_mph, default = NA), 
         next_accel = lead(prev_accel, default = NA)
         ) %>%
  ungroup() %>%
  mutate_at(vars(door_state,vehicle_state),
            factor) %>%
  fastDummies::dummy_cols(select_columns = c("door_state","vehicle_state")) %>%
  st_as_sf()

#Grab GTFS data for later
#NB: Pulled for a few weeks later than I meant to (should have been 
#May 1, 2019, but I instead grabbed May 19, 2019), shouldn't be too 
#problematic for this exercise ()
gtfs_obj <- 
  tidytransit::read_gtfs(
    path = file.path(datadir,
                     "wmata-2019-05-18 dl20200205gtfs.zip")
  )

stops <-
  gtfs_obj$stops %>%
  st_as_sf(., 
           coords = c("stop_lon", "stop_lat"),
           crs = 4326L, #WGS84
           agr = "constant")

#Note, setting aside any weird calendar business, this is a quickie...
#could also do for all routes/stops, but want to be a little more conservative
#on memory in a map-laden notebook. 
get_route_stops <- function(gtfs_obj,
                            stops,
                            rt_shrt_nm){
  out <- 
    gtfs_obj$routes %>%
    filter(route_short_name == rt_shrt_nm) %>%
    left_join(gtfs_obj$trips, by = "route_id") %>%
    left_join(gtfs_obj$stop_times, by = "trip_id") %>%
    group_by(route_short_name,direction_id,trip_headsign,shape_id,stop_id) %>%
    summarize(trips = length(unique(trip_id)),
              #not a great assumption, but this is a quickie
              stop_sequence = max(stop_sequence)) %>%
    left_join(stops, by = "stop_id") %>%
    arrange(stop_sequence, .by_group = TRUE) %>%
    ungroup() %>%
    st_sf() 
}
