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

ad_decomp <-
  read_csv(file = file.path(path_data,
                            "ad_method_decomp.csv"),
           col_types = 
             cols(
               .default = col_double(),
               filename = col_character(),
               seg_name_id = col_character(),
               fullpath = col_character(),
               route = col_character(),
               route_pattern = col_character(),
               file_id = col_character(),
               taglist = col_character(),
               start_date_time = col_datetime(format = ""),
               end_date_time = col_datetime(format = ""),
               run_duration_from_tags = col_character(),
               wday = col_character(),
               secs_t_stop = col_number()
             )) %>%
  filter(!(seg_name_id %in% c("georgia_irving","nh_3rd_test")))
  

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

og_colors_timespace = c(
  "t_decel_phase" = "#D7191C",
  "t_l_initial" = "#FDAE61",
  "t_stop1" = "#FFFFBF",
  "t_stop" = "#FFFFBF", 
  "t_l_addl" = "#A6D96A",
  "t_accel_phase" = "#1A9641",
  "t_nostopnopax" = "#6a3d9a"
)

new_colors_timespace = c(
  "t_decel_phase" = "#a6611a",
  "t_l_initial" = "#dfc27d",
  "t_stop1" = "#FFFFBF",
  "t_stop" = "#FFFFBF", 
  "t_l_addl" = "#80cdc1",
  "t_accel_phase" = "#018571",
  "t_nostopnopax" = "#018571"
)

og_color_speeddist = c(
    "t_decel_phase" = "#D7191C",
    "t_l_initial" = "#FDAE61",
    "t_stop1" = "#FFFFBF",
    "t_stop" = "#FFFFBF",
    "t_l_addl" = "#A6D96A",
    "t_accel_phase" = "#1A9641",
    "t_nostopnopax" = "#6a3d9a"
  )

new_color_speeddist = c(
  "t_decel_phase" = "#a6611a",
  "t_l_initial" = "#dfc27d",
  "t_stop1" = "#FFFFBF",
  "t_stop" = "#FFFFBF",
  "t_l_addl" = "#80cdc1",
  "t_accel_phase" = "#1A9641",
  "t_nostopnopax" = "#018571"
)

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
      values = new_colors_timespace
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
      values = new_color_speeddist
    ) + 
    scale_x_continuous(labels = scales::comma) +
    scale_y_continuous(limits = c(0, 40), n.breaks = 5) +
    labs(x = "Dist. (ft) in Stop Area",
         y = "Speed (mph) next 3 obs.") +
    guides(color = guide_legend(reverse = TRUE)) +
    theme(legend.position = legend)
  
}

combine_basic_chart <- function(case,seg){
  ts <- rawnav_stop_area %>%
    filter(busrun == case, seg_name_id == seg) %>%
    make_basic_timespace()
  
  sd <- rawnav_stop_area %>%
    filter(busrun == case, seg_name_id == seg) %>%
    make_basic_speeddist(alpha = .5, 
                         size = 1.5, 
                         lt = "solid",
                         legend = "none")
  
  combined_basic_chart <- (ts / sd)
  
  return(combined_basic_chart)
}

decomp_stack <- 
  function(decomp){
    # set cols
    ltblue = "#A6CEE3"
    dkblue = "#1F78B4"
    ltgrn = "#B2DF8A"
    dkgrn = "#33A02C"
    ltred = "#FB9A99"
    dkred = "#E31A1C"
    ltorg = "#FDBF6F"
    dkorg = "#FF7F00"  
    # browser()
    # data check
    chartdata <-
      decomp %>%
      select(
        busrun,
        seg_name_id,
        ts_approach_min,
        tr_approach_delay,
        tr_init_wait,
        tr_stop1,
        tr_stop,
        tr_signal_wait,
        ts_accel_min,
        tr_accel_delay,
        ts_nostop_min,
        tr_nostop_delay
      ) %>%
      pivot_longer(
        names_to = "decomp_state",
        values_to = "secs",
        cols = ts_approach_min:tr_nostop_delay
      ) %>%
      mutate(secs = round(secs,0),
             decomp_state = 
               factor(decomp_state,
                      levels = c("ts_approach_min",
                                 "tr_approach_delay",
                                 "tr_init_wait",
                                 "tr_stop1",
                                 "tr_stop",
                                 "tr_signal_wait",
                                 "ts_accel_min",
                                 "tr_accel_delay",
                                 "ts_nostop_min",
                                 "tr_nostop_delay")
               ),
             source = if_else(decomp_state %in% c("ts_approach_min",
                                                  "ts_accel_min",
                                                  "ts_nostop_min"),
                              "Segment-level",
                              "Run-level")
      ) 
    
    # make cahrt
    ggplot(chartdata,
           aes(x = busrun,
               y = secs, 
               group = busrun, 
               fill = decomp_state,
               color = source,
               label = secs)) + 
      geom_col(size = 1) + 
      geom_label(position = position_stack(vjust = .5)) +
      scale_fill_manual(
        values = c(
          "ts_approach_min" = dkred,
          "tr_approach_delay" = ltred,
          "tr_init_wait" = ltblue, 
          "tr_stop1" = dkblue,
          "tr_stop" = dkblue,
          "tr_signal_wait" = ltorg,
          "ts_accel_min" = dkgrn,
          "tr_accel_delay" = ltgrn,
          "ts_nostop_min" = dkred,
          "tr_nostop_delay" = ltred
        )
      ) + 
      scale_color_manual(
        values = c("Segment-level" = "black",
                   "Run-level" = "#6a3d9a" )
      ) +
      guides(color = guide_legend(reverse = TRUE)) +
      labs(y = "Seconds")
    
  }

doublecheck <- function(case, seg){
  
  dc <- 
    stop_pass_decomp_4 %>%
    filter(busrun == case, seg_name_id == seg) %>%
    decomp_stack()
  
  combined_basic <- combine_basic_chart(case, seg)
  
  nonstoparea_subseg_table <- 
    rawnav_nonstop_area %>%
    filter(busrun == case, seg_name_id == seg) %>%
    mutate(segment_part = str_replace_all(segment_part,
                                          c("after_stop_area" = "After Stop Area",
                                            "before_stop_area" = "Before Stop Area"))) %>%
    select(-busrun, -seg_name_id) %>%
    gridExtra::tableGrob()
  
  bigplot <-
    ((combined_basic | dc) / nonstoparea_subseg_table) +
    plot_annotation(glue::glue("{seg}: Results for run {case}"))
}
