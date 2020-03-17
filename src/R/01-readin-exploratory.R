#*******************************************************************************
#PROJECT:       WMATA AVL
#DATE CREATED:  Tue Mar 17 04:33:59 2020
#TITLE:         Data Read in
#AUTHOR:        Wylie Timmerman (wtimmerman@foursquareitp.com)
#BACKGROUND:    Loads in some GTFS data 
#*******************************************************************************

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