magic <-
  rawnav_stop_area %>%
  filter(seg_name_id == "georgia_columbia") %>%
  filter(stop_area_state != "t_nostop") %>%
  group_by(busrun, seg_name_id) %>%
  filter(
    any(stop_area_state == c("t_l_initial"))
    & any(stop_area_state == c("t_l_addl"))
  )
    
    all(any(stop_area_state %in% c("t_decel_phase",
                                        "t_l_initial",
                                        "t_stop1",
                                        "t_l_addl",
                                        "t_accel_phase"))))


candidates <- magic %>%
  group_by(busrun, stop_area_state) %>%
  summarize(tot = sum(secs_marg, na.rm = TRUE))

helpmehelpyou <- 
  rawnav_stop_area %>%
  filter(busrun == "rawnav06037191005.txt_9435") 
