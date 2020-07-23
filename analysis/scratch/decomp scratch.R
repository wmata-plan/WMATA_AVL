freeflow_to_join <-
  freeflow %>%
  filter(ntile == .95) %>%
  select(seg_name_id,
         freeflow_fps = fps_next3)

# Let's add in the decel phase here instead
no_stop_decomp <-
  rawnav_stop_area %>%
  # Though we're not expecting to find other values in this category, we'll 
  # be a little careful still
  group_by(busrun, seg_name_id) %>%
  filter(stop_area_state %in% c("t_nostop","t_decel_phase")) %>% 
  group_by(busrun, seg_name_id) %>%
  summarize(
            delay_source = paste0(unique(stop_area_state), collapse =","),
            tot_secs = sum(secs_marg, na.rm = TRUE),
            tot_odom_ft = sum(odom_ft_marg, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(freeflow_to_join, by = "seg_name_id") %>%
  mutate(freeflow_secs = tot_odom_ft / freeflow_fps,
         misc_delay_secs = tot_secs - freeflow_secs) %>%
  select(busrun, 
         seg_name_id,
         delay_source,
         freeflow_secs,
         misc_delay_secs)

# TODO: rename to accel time, modify some of this
stop_time <-
  rawnav_stop_area %>%
  group_by(busrun, seg_name_id) %>%
  filter(!any(stop_area_state %in% c("t_nostop"))) %>%
  mutate(decelaccel = stop_area_state %in% c(#"t_decel_phase", #TODO: update this to be accel only?
                                             "t_accel_phase")) %>%
  filter(decelaccel) %>%
  group_by(busrun, seg_name_id) %>%
  summarize(tot_stopping_secs = sum(secs_marg, na.rm = TRUE),
            tot_stopping_odom_ft = sum(odom_ft_marg, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(stopping_fps = tot_stopping_odom_ft / tot_stopping_secs) 

low_stop_time_distr <-
  stop_time %>%
  group_by(seg_name_id) %>%
  summarize(distr = list(quantile(stopping_fps, probs = c(.02,.05,.1,.25,.5,.75,.9,.95,.98)))) 

low_stop_time_to_join <- 
  low_stop_time_distr %>%
  unnest_longer(col = distr,
                values_to = "stopping_fps",
                indices_to  = "ntile") %>%
  filter(ntile == "95%") %>%
  select(seg_name_id, 
         low_stopping_fps = stopping_fps)

stop_time2 <-
  stop_time %>%
  left_join(low_stop_time_to_join, by = "seg_name_id") %>%
  mutate(lowstopping_secs = tot_stopping_odom_ft / low_stopping_fps,
         queue_delay_secs = tot_stopping_secs - lowstopping_secs)

# bring it all together
stop_pass_decomp_1 <- 
  rawnav_stop_area %>%
  # Though we're not expecting to find other values in this category, we'll 
  # be a little careful still
  group_by(busrun, seg_name_id) %>%
  # filter(!any(stop_area_state %in% c("t_nostop"))) %>%  # TODO: drop this later?
  group_by(busrun, seg_name_id, stop_area_state) %>%
  summarize(tot_secs = sum(secs_marg, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_wider(names_from = "stop_area_state", values_from = "tot_secs") %>%
  rowwise() %>%
  mutate(tot_secs =
           sum(
           t_decel_phase, 
           t_l_initial,
           t_stop1,
           t_l_addl,
           t_accel_phase,
           t_nostop,
           na.rm = TRUE
           )) %>%
  ungroup() %>%
  select(busrun,
         seg_name_id,
         t_decel_phase,
         t_l_initial,
         t_stop1,
         t_l_addl,
         t_accel_phase,
         t_nostop,
         tot_secs)

stop_pass_decomp_2 <-
  stop_pass_decomp_1 %>%
  left_join(no_stop_decomp, by = c("busrun","seg_name_id")) %>%
  left_join(stop_time2, by = c("busrun","seg_name_id")) 


stop_pass_decomp_3 <-
  stop_pass_decomp_2 %>%
  mutate(ts_approach_min = freeflow_secs,
         tr_approach_delay = misc_delay_secs,
         tr_init_wait = t_l_initial,
         tr_stop1 = t_stop1,
         tr_signal_wait = t_l_addl,
         ts_accel_min = lowstopping_secs,
         tr_accel_delay = queue_delay_secs
         ) %>%
  rowwise() %>%
  mutate(tot_secs_check = 
           sum(
             ts_approach_min, 
             tr_approach_delay,
             tr_init_wait,
             tr_stop1,
             tr_signal_wait,
             ts_accel_min,
             tr_accel_delay,
             na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    tot_secs_check = round(tot_secs_check),
    checksout = tot_secs == tot_secs_check
    )
  
# TODO: 
# Test that the 
testthat::test_that("check sum matches for everything",{
  doesnt_checkout <-
    stop_pass_decomp_3 %>%
    filter(!checksout)
  
  testthat::expect_true(nrow(doesnt_checkout) == 0)
})

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
      tr_signal_wait,
      ts_accel_min,
      tr_accel_delay
    ) %>%
    pivot_longer(
      names_to = "decomp_state",
      values_to = "secs",
      cols = ts_approach_min:tr_accel_delay
    ) %>%
    mutate(secs = round(secs,0),
           decomp_state = 
             factor(decomp_state,
                    levels = c("ts_approach_min",
                               "tr_approach_delay",
                               "tr_init_wait",
                               "tr_stop1",
                               "tr_signal_wait",
                               "ts_accel_min",
                               "tr_accel_delay")),
           source = if_else(decomp_state %in% c("ts_approach_min",
                                                "ts_accel_min"),
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
        "tr_signal_wait" = ltorg,
        "ts_accel_min" = dkgrn,
        "tr_accel_delay" = ltgrn
        )
      ) + 
      scale_color_manual(
        values = c("Segment-level" = "black",
                   "Run-level" = "#6a3d9a" )
      ) +
      guides(color = guide_legend(reverse = TRUE)) +
      labs(y = "Seconds")
    
  }

# DEBUG SPEED STUFF
case <- "rawnav03235191030.txt_26551"
case <- "rawnav03259191029.txt_28005"
case <- "rawnav03232191024.txt_9587"

stop_pass_decomp_3 %>%
  filter(busrun == "rawnav03232191024.txt_9587") %>%
  filter(seg_name_id == first(seg_name_id)) %>%
  decomp_stack

doublecheck <- function(case, seg){
  # browser()
  dc <- 
    stop_pass_decomp_3 %>%
    filter(busrun == case, seg_name_id == seg) %>%
    decomp_stack()
  
  ts <- rawnav_stop_area %>%
    filter(busrun == case, seg_name_id == seg) %>%
    make_basic_timespace()
  
  sd <- rawnav_stop_area %>%
    filter(busrun == case, seg_name_id == seg) %>%
    make_basic_speeddist(alpha = .5, 
                         size = 1.5, 
                         lt = "solid",
                         legend = "none")
  
  ((ts / sd) | dc) +
    plot_annotation(glue::glue("{seg}: Results for run {case}"))
}


set.seed(1)

frame <-
  rawnav_stop_area %>%
  distinct(busrun, seg_name_id) %>%
  sample_n(size = 10) %>%
  mutate(
    busrunshow = str_replace(busrun, '\\.txt', 'txt'),
    filename = glue::glue("{seg_name_id}_{busrunshow}.png"),
    plot = pmap(list(busrun, seg_name_id),
                doublecheck)
  )

height = 7
asp = 1.6

frame %>%
  select(filename,
         plot) %>%
  head() %>%
  pwalk(.,
        ggsave,
        path = file.path(path_project,'data','01-interim','images'),
        width = height * asp,
        height = height,
        units = "in",
        scale = .9,
        dpi = 300)


