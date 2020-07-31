calc_phase_delay <- function(rawnav_stop_area,
                             phase = "t_accel_phase",
                             name = "accel"){
  
  #TODO: if in the 'no stop' case, maybe run 95th percentile for all trips
  # and then difference this.
  
  speed_1 <-
    rawnav_stop_area %>%
    filter(stop_area_phase %in% phase) %>%
    group_by(busrun, seg_name_id) %>%
    summarize("tot_{name}_secs" := sum(secs_marg, na.rm = TRUE),
              "tot_{name}_odom_ft" := sum(odom_ft_marg, na.rm = TRUE)) %>%
    ungroup() %>%
    # to be safe, we set any fps values of NaN or Inf to NA -- probably a case where 
    # the stop zone is defined too tightly and we get weird cases with inf speed
    mutate("{name}_fps" := 
             .data[[glue::glue("tot_{name}_odom_ft")]] 
           / na_if(.data[[glue("tot_{name}_secs")]],0)) 
  
  speed_1_distr <-
    speed_1 %>%
    group_by(seg_name_id) %>%
    summarize(distr = list(quantile(.data[[glue::glue("{name}_fps")]], 
                                    probs = c(.02,.05,.1,.25,.5,.75,.9,.95,.98),
                                    na.rm = TRUE))) 
  
  low_speed_to_join <- 
    speed_1_distr %>%
    unnest_longer(col = distr,
                  values_to = glue::glue("{name}_fps"),
                  indices_to  = "ntile") %>%
    filter(ntile == "95%") %>%
    select(seg_name_id, 
           "low_{name}_fps" := .data[[glue::glue("{name}_fps")]])
  
  speed_2 <-
    speed_1 %>%
    left_join(low_speed_to_join, by = "seg_name_id") %>%
    mutate("min_{name}_secs" := .data[[glue::glue("tot_{name}_odom_ft")]] / .data[[glue::glue("low_{name}_fps")]],
           "addl_{name}_secs" := .data[[glue::glue("tot_{name}_secs")]] - .data[[glue::glue("min_{name}_secs")]]) %>%
    select(busrun, seg_name_id,glue::glue("min_{name}_secs"), glue::glue("addl_{name}_secs"))
  
}

# No stop phase -------
no_stop_time <- calc_phase_delay(rawnav_stop_area,
                                 "t_nostopnopax",
                                 "nostopnopax")

# Decel Phase -----
decel_time <- calc_phase_delay(rawnav_stop_area,
                               "t_decel_phase",
                               "decel")

# Accel Phase -----
accel_time <- calc_phase_delay(rawnav_stop_area,
                               "t_accel_phase",
                               "accel")

# bring it all together ----
stop_pass_decomp_1 <- 
  rawnav_stop_area %>%
  group_by(busrun, seg_name_id, stop_area_phase) %>%
  summarize(tot_secs = sum(secs_marg, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_wider(names_from = "stop_area_phase", values_from = "tot_secs") %>%
  rowwise() %>%
  mutate(tot_secs =
           sum(
             t_decel_phase, 
             t_l_initial,
             t_stop1,
             t_stop,
             t_l_addl,
             t_accel_phase,
             t_nostopnopax,
             na.rm = TRUE
           )) %>%
  ungroup() %>%
  select(busrun,
         seg_name_id,
         t_decel_phase,
         t_l_initial,
         t_stop1,
         t_stop,
         t_l_addl,
         t_accel_phase,
         t_nostopnopax,
         tot_secs)

stop_pass_decomp_2 <-
  stop_pass_decomp_1 %>%
  left_join(no_stop_time, by = c("busrun","seg_name_id")) %>%
  left_join(decel_time,   by = c("busrun","seg_name_id")) %>%
  left_join(accel_time,   by = c("busrun","seg_name_id")) 

stop_pass_decomp_3 <-
  stop_pass_decomp_2 %>%
  mutate(ts_approach_min = min_decel_secs,
         tr_approach_delay = addl_decel_secs,
         tr_init_wait = t_l_initial,
         tr_stop = t_stop,
         tr_stop1 = t_stop1,
         tr_signal_wait = t_l_addl,
         ts_accel_min = min_accel_secs,
         tr_accel_delay = addl_accel_secs,
         ts_nostop_min = min_nostopnopax_secs,
         tr_nostop_delay = addl_nostopnopax_secs
  ) %>%
  rowwise() %>%
  mutate(tot_secs_check = 
           sum(
             ts_approach_min, 
             tr_approach_delay,
             tr_init_wait,
             tr_stop1,
             tr_stop,
             tr_signal_wait,
             ts_accel_min,
             tr_accel_delay,
             ts_nostop_min,
             tr_nostop_delay,
             na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    tot_secs_check = round(tot_secs_check),
    checksout = tot_secs == tot_secs_check
  )

rawnav_nonstop_area_wide <-
  rawnav_nonstop_area %>%
  pivot_wider(id_cols = busrun:seg_name_id,
              names_from = segment_part,
              values_from = subsegment_min_sec:subsegment_delay_sec)

stop_pass_decomp_4 <- 
  stop_pass_decomp_3 %>%
  left_join(rawnav_nonstop_area_wide, by = c("busrun","seg_name_id"))


# Test that the 
testthat::test_that("check sum matches for everything",{
  doesnt_checkout <-
    stop_pass_decomp_4 %>%
    filter(!checksout)
  
  testthat::expect_true(nrow(doesnt_checkout) == 0)
})
