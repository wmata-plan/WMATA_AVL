# Let's calculate the runtime and delay for runs that don't stop at all. 
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
  filter(stop_area_phase %in% c("t_nostopnopax","t_decel_phase")) %>%  
  group_by(busrun, seg_name_id) %>%
  summarize(
    delay_source = paste0(unique(stop_area_phase), collapse =","),
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