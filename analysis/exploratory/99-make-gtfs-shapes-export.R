#*******************************************************************************
#PROJECT:       WMATA AVL
#DATE CREATED:  Tue Jun 16 02:54:48 2020
#TITLE:         Create a set of shapes to export
#AUTHOR:        Wylie Timmerman (wtimmerman@foursquareitp.com
#PURPOSE:       reads in GTFS feed, creates shapes for data driven pages
#*******************************************************************************


# Pakcages ----------------------------------------------------------------


library(tidyverse)
library(tidytransit)
library(sf)



# Environ -----------------------------------------------------------------

if ("WylieTimmerman" %in% Sys.info()){
  sharepointpath <- "C:/OD/Foursquare ITP/Foursquare ITP SharePoint Site - Shared Documents/WMATA Queue Jump Analysis"
  datadir <- file.path(sharepointpath,
                       "Client Shared Folder",
                       "data",
                       "00-raw")
  
} else {
  stop('need to set dir where files live')
}


# Define -----------------------------------------------------------------

# segment-name
seg_route_xwalk <-
  tribble(
    ~route_short_name, ~direction_id,                   ~seg_name_id,
                 "79",            1,             "georgia_columbia",
                 "79",            1,         "georgia_piney_branch_long",
                 "70",            1,               "georgia_irving",
                 "70",            1,         "georgia_piney_branch_shrt",
                 "S1",            0,                  "sixteenth_u_shrt",
                 "S2",            0,                  "sixteenth_u_shrt",
                 "S4",            0,                  "sixteenth_u_shrt",
                 "S9",            0,                  "sixteenth_u_long",
                 "64",            0,          "eleventh_i_new_york",  
                 "G8",            0,          "eleventh_i_new_york", 
                "D32",            0,   "irving_fifteenth_sixteenth",
                 "H1",            0,   "irving_fifteenth_sixteenth",
                 "H2",            0,   "irving_fifteenth_sixteenth",
                 "H3",            0,   "irving_fifteenth_sixteenth",
                 "H4",            0,   "irving_fifteenth_sixteenth",
                 "H8",            0,   "irving_fifteenth_sixteenth",
                "W47",            0,   "irving_fifteenth_sixteenth"
  )

#TODO: add nicename?  
segments <-
  tribble(
                      ~seg_name_id,    ~seg_dir,     ~peak_time,
                "georgia_columbia",        "SB",      "AM_Peak",
                  "georgia_irving",        "SB",      "AM_Peak",
       "georgia_piney_branch_shrt",        "SB",      "AM_Peak",
       "georgia_piney_branch_long",        "SB",      "AM_Peak",
                "sixteenth_u_shrt",        "NB",      "PM_Peak",
                "sixteenth_u_long",        "NB",      "PM_Peak",
             "eleventh_i_new_york",        "NB",      "PM_Peak",
      "irving_fifteenth_sixteenth",        "EB",      "PM_Peak"
  )



# Make Stops --------------------------------------------------------------

gtfs_obj <- 
  tidytransit::read_gtfs(
    path = file.path(datadir,
                     "wmata-2019-05-18 dl20200205gtfs.zip")
  )

out <-
  gtfs_obj$routes %>%
  left_join(gtfs_obj$trips, by = "route_id") %>%
  inner_join(seg_route_xwalk, by = c("route_short_name","direction_id")) %>%
  left_join(gtfs_obj$stop_times, by = "trip_id") %>%
  left_join(gtfs_obj$stops, by = "stop_id") %>%
  group_by(route_short_name, direction_id, trip_id) %>%
  mutate(pattern = paste0(stop_id, collapse = ", "),
         pattern_length = n()) %>%
  group_by(seg_name_id, route_short_name, direction_id, trip_headsign, pattern, pattern_length, stop_id) %>%
  summarize(stop_sequence = first(stop_sequence),
            stop_lon = first(stop_lon),
            stop_lat = first(stop_lat)) %>%
  group_by(seg_name_id, route_short_name,direction_id, trip_headsign, pattern) %>%
  arrange(stop_sequence, .by_group = TRUE)  %>%
  st_as_sf(., 
           coords = c("stop_lon", "stop_lat"),
           crs = 4326L, #WGS84
           agr = "constant")

write_sf(out,
         dsn = file.path(sharepointpath, 
                         "Client Shared Folder",
                         "data",
                         "02-processed",
                         "stops_by_route_dir_segment_v2.geojson"))

