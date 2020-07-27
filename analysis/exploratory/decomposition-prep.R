# Environment setup
library(tidyverse)
library(plotly)
library(sf)
library(mapview)
library(leaflet)
library(patchwork)
library(glue)
library(gridExtra)

set.seed(1)

if (any(Sys.info() =="WylieTimmerman")){
  path_project <- "C:/Users/WylieTimmerman/Documents/projects_local/wmata_avl_local"  
} else {
  stop('set paths')
}

path_data <- file.path(path_project,"data","02-processed","rawnav_stop_areas")

rawnav_stop_area_raw <- 
  readr::read_csv(file.path(path_project,
                            "data",
                            "02-processed",
                            "rawnav_stop_areas",
                            "basic_decomp.csv"),
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
                  ))


  

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

# Additional Work on non stop area vals
rawnav_nonstop_area_raw <-
  readr::read_csv(file.path(path_project,
                            "data",
                            "02-processed",
                            "rawnav_stop_areas",
                            "nonstopzone_ff.csv"))

rawnav_nonstop_area <-
  rawnav_nonstop_area_raw %>%
  unite(busrun, filename, index_run_start, remove = TRUE) %>%
  select(busrun, seg_name_id, segment_part, subsegment_min_sec, subsegment_delay_sec) %>%
  mutate(subsegment_min_sec = round(subsegment_min_sec,0),
         subsegment_delay_sec = round(subsegment_delay_sec,0)) 


# Key Functions -----------------------------------------------------------

make_basic_timespace <- function(df){
  ggplot(df,
         aes(x = secs_stop_area, 
             y = odom_ft_stop_area, 
             color = stop_area_phase,
             group = busrun,
             label = stop_area_phase)) + 
    geom_line(alpha = 1, size = 2) + 
    scale_y_continuous(labels = scales::comma,
                       name = "Dist (ft) in Stop Area") +
    scale_x_continuous(labels = scales::comma,
                       name = "Time (secs) in Stop Area") +
    scale_color_manual(
      values = c(
        "t_decel_phase" = "#D7191C",
        "t_l_initial" = "#FDAE61",
        "t_stop1" = "#FFFFBF",
        "t_stop" = "#FFFFBF", 
        "t_l_addl" = "#A6D96A",
        "t_accel_phase" = "#1A9641",
        "t_nostopnopax" = "#6a3d9a"
      )
    ) +
    guides(color = guide_legend( direction = "horizontal",
                                 nrow = 2,
                                 title = NULL,
                                 byrow = TRUE)) +
    theme(legend.position = "top")
}

make_basic_speeddist <- function(df, alpha = .1, size = 1, lt = "solid", legend = "right"){
  ggplot(df,
         aes(x = odom_ft_stop_area,
             y = mph_next3, 
             group = busrun, 
             color = stop_area_phase)) + 
    geom_line(alpha = alpha, 
              size = size,
              linetype = lt) + 
    scale_color_manual(
      values = c(
        "t_decel_phase" = "#D7191C",
        "t_l_initial" = "#FDAE61",
        "t_stop1" = "#FFFFBF",
        "t_stop" = "#FFFFBF",
        "t_l_addl" = "#A6D96A",
        "t_accel_phase" = "#1A9641",
        "t_nostopnopax" = "#6a3d9a"
      )
    ) + 
    scale_x_continuous(labels = scales::comma) +
    scale_y_continuous(limits = c(0, 40), n.breaks = 5) +
    labs(x = "Dist. (ft) in Stop Area",
         y = "Speed (mph) next 3 obs.") +
    guides(color = guide_legend(reverse = TRUE)) +
    theme(legend.position = legend)
  
}
