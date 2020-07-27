# Environment setup
library(tidyverse)
library(plotly)
library(sf)
library(mapview)
library(leaflet)
library(patchwork)
library(glue)

set.seed(1)

if (any(Sys.info() =="WylieTimmerman")){
  path_project <- "C:/Users/WylieTimmerman/Documents/projects_local/wmata_avl_local"  
} else {
  stop('set paths')
}

path_data <- file.path(path_project,"data","02-processed","rawnav_stop_areas")

filelist <- list.files(path_data, pattern = "ourpoints", full.names = TRUE)

filelist_named <- set_names(filelist,
                            nm = str_match(filelist,"ourpoints_([\\s\\S]*).csv$")[,2])

rawnav_stop_area_raw <- 
  imap_dfr(filelist_named,
           ~ readr::read_csv(.x,
                             col_types =  cols(
                               X1 = col_double(),
                               index = col_double(),
                               index_loc = col_double(),
                               lat = col_double(),
                               long = col_double(),
                               heading = col_double(),
                               door_state = col_character(),
                               veh_state = col_character(),
                               odom_ft = col_double(),
                               sec_past_st = col_double(),
                               stop_window = col_character(),
                               row_before_apc = col_double(),
                               route_pattern = col_character(),
                               pattern = col_double(),
                               index_run_start = col_double(),
                               index_run_end = col_double(),
                               filename = col_character(),
                               start_date_time = col_datetime(format = ""),
                               route = col_double(),
                               wday = col_character(),
                               odom_ft_qj_stop = col_double(),
                               stop_id = col_double(),
                               odom_ft_next = col_double(),
                               sec_past_st_next = col_double(),
                               odom_ft_next3 = col_double(),
                               sec_past_st_next3 = col_double(),
                               fps_next = col_double(),
                               fps_next3 = col_double(),
                               secs_marg = col_double(),
                               odom_ft_marg = col_double(),
                               door_state_closed = col_logical(),
                               veh_state_moving = col_logical(),
                               veh_state_changes = col_double(),
                               door_state_changes = col_double(),
                               rough_state = col_character(),
                               at_stop = col_logical(),
                               at_stop_state = col_logical(),
                               stop_area_phase = col_character()
                             )),
           .id = "seg_name_id"
  )

# some light calculation of fields and filtering
rawnav_stop_area <-
  rawnav_stop_area_raw %>%
  unite(busrun, filename, index_run_start, remove = FALSE) %>%
  mutate(mph_next = fps_next / 1.467,
         mph_next3 = fps_next3 / 1.467) %>%
  mutate(stop_area_phase = factor(stop_area_phase,
                                  levels = c("t_decel_phase",
                                             "t_l_initial",
                                             "t_stop1",
                                             "t_stop",
                                             "t_l_addl",
                                             "t_accel_phase",
                                             "t_nostopnopax",
                                             "t_nopax"))) %>%
  group_by(busrun, seg_name_id) %>%
  mutate(odom_ft_stop_area = odom_ft - min(odom_ft),
         secs_stop_area = sec_past_st - min(sec_past_st)) %>%
  # there also issues with odometer resets in some cases
  filter(max(secs_stop_area) < 500) %>%
  ungroup() %>%
  # issues with this one b/c of multiple stops, setting aside for now
  filter(seg_name_id != "georgia_irving")

testthat::test_that('no t_nopax found',{
  # all of these cases for runs that didn't pick up passengers in a zone
  # should be overwritten with a state based on when the vehicle stopped.
  t_nopax_found <- 
    rawnav_stop_area %>%
    filter(stop_area_phase == "t_nopax")
  
  testthat::expect_equal(nrow(t_nopax_found),0)
  
})
