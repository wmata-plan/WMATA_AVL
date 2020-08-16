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
             ))

tt_decomp <-
  read_csv(file = file.path(path_data,
                            "traveltime_decomp.csv"),
           col_types = 
             cols(
               .default = col_double(),
               filename = col_character(),
               fullpath = col_character(),
               file_id = col_character(),
               taglist = col_character(),
               route = col_character(),
               route_pattern = col_character(),
               start_date_time = col_datetime(format = ""),
               end_date_time = col_datetime(format = ""),
               run_duration_from_tags = col_character(),
               wday = col_character(),
               seg_name_id = col_character()
             ))


compare_df <-
  ad_decomp %>%
  select(filename:secs_total,total_diff, secs_t_stop) %>%
  right_join(
    select(tt_decomp,
           filename:t_traffic,
           route,
           route_pattern,
           wday,
           start_date_time,
           seg_name_id),
    by = c("filename","index_run_start",'seg_name_id')
  ) %>%
  unite(busrun, filename, index_run_start, remove = FALSE) %>%
  mutate(method_secs_diff = secs_seg_total - secs_total) %>%
  relocate(total_diff, .after = method_secs_diff) %>%
  filter(abs(total_diff) < 2)
