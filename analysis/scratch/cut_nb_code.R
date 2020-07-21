
## 2. Item 2
A handful of trips have impossibly high time in the stop areas, but it looks like they may have had their odometer's reset mid-trip. Though some 
```{r}
rawnav_stop_area_raw %>%
  filter(busrun == "rawnav06466191019.txt_3515")

```

What's going on with resetting the distance in zone or long timeframes in zone? Seems like the odometer is off on the busrun, may have reset itself partway along. We had tried to 'cheat' on using the odometer as a way to split out the stop zone, but maybe this isn't a safe assumption.

```{r}
toolong <- 
  rawnav_stop_area %>%
  filter(seg_name_id == "georgia_columbia") %>%
  group_by(busrun,seg_name_id) %>%
  filter(max(secs_stop_area) > 500) %>%
  ungroup() %>%
  filter(busrun == first(busrun))

```

From looking at the map, it may be that the odometer is incorrect for this bus run, and reset itself partway along. We don't have the full rawnav data loaded at the moment, so we can't examine further.
```{r}
toolongsf <-
  toolong %>%
  st_as_sf(.,
           coords = c("long", "lat"),
           crs = 4326L, #WGS84
           agr = "constant")

leaflet(toolongsf) %>%
  leaflet::addC(zcol = "odom_ft")
```

