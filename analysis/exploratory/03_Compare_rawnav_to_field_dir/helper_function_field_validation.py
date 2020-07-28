import numpy as np, pandas as pd, os
import wmatarawnav as wr

# Hard coding things---Not fit for sharing
path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents" \
                   r"\WMATA-AVL\Data\field_data_rawnav_data_july_week_2_and_3"
# Processed data
path_processed_data = os.path.join(path_source_data, "processed_data")
path_wmata_schedule_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data" \
                           r"\wmata_schedule_data\Schedule_082719-201718.mdb"
q_jump_route_list = ['S1', 'S2', 'S4', 'S9',
                     '70', '79',
                     '64', 'G8',
                     'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
# If desired, a subset of routes above or the entire list. Code will iterate on the analysis_routes list
analysis_routes = q_jump_route_list


def time_to_sec(time_mess):
    hr_min_sec = str(time_mess).split(":")
    if len(hr_min_sec) == 3:
        tot_sec = int(hr_min_sec[0]) * 3600 + int(hr_min_sec[1]) * 60 + int(hr_min_sec[2])
        return (tot_sec)
    if len(hr_min_sec) == 2:
        if hr_min_sec[0].strip() == '':
            tot_sec = hr_min_sec[1]
            return (int(tot_sec))
    elif len(hr_min_sec) == 1:
        tot_sec = hr_min_sec[0]
        if tot_sec.isnumeric():
            return (np.int(tot_sec))
        else:
            return (np.nan)
    else:
        ""
    raise ("Can't handle argument")


def correct_data_types(dat):
    dat = dat.assign(metrobus_route_field=lambda x: x.metrobus_route_field.astype("str"),
                              bus_id_field =lambda x: x.bus_id_field.astype("Int32"),
                              date_obs_field = lambda x: x.date_obs_field.astype("str"),
                              signal_phase_field = lambda x: x.signal_phase_field.astype('str'),
                              time_entered_stop_zone_field = lambda x: pd.to_datetime(x.date_obs_field+" "+x.time_entered_stop_zone_field.astype(str),infer_datetime_format=True,errors='coerce'),
                              time_left_stop_zone_field = lambda x: pd.to_datetime(x.date_obs_field+" "+x.time_left_stop_zone_field.astype(str),infer_datetime_format=True,errors='coerce'),
                              front_door_open_time_field = lambda x: pd.to_datetime(x.date_obs_field+" "+x.front_door_open_time_field.astype(str),infer_datetime_format=True,errors='coerce'),
                              front_door_close_time_field = lambda x: pd.to_datetime(x.date_obs_field+" "+x.front_door_close_time_field.astype(str),infer_datetime_format=True,errors='coerce'),
                              rear_door_open_time_field = lambda x: pd.to_datetime(x.date_obs_field+" "+x.rear_door_open_time_field.astype(str),infer_datetime_format=True,errors='coerce'),
                              rear_door_close_time_field = lambda x: pd.to_datetime(x.date_obs_field+" "+x.rear_door_close_time_field.astype(str),infer_datetime_format=True,errors='coerce'),
                              dwell_time_field =lambda x: pd.to_timedelta(x.dwell_time_field.apply(time_to_sec),unit='s'),
                              number_of_boarding_field = lambda x: x.number_of_boarding_field.astype('Int32'),
                              number_of_alightings_field = lambda x: x.number_of_alightings_field.astype('Int32'),
                              total_time_at_intersection_field =lambda x: pd.to_timedelta(x.total_time_at_intersection_field.apply(time_to_sec),unit='s'),
                              traffic_conditions_field = lambda x: x.traffic_conditions_field.astype('str'),
                              notes_field = lambda x: x.notes_field.astype('str')
                             )
    return dat


def quick_and_dirty_schedule_qjump_mapping():
    xwalk_seg_pattern_stop_in = wr.tribble(
        ['route', 'direction', 'seg_name_id', 'stop_id'],
        "79", "SOUTH", "georgia_columbia", 10981,
        "79", "SOUTH", "georgia_piney_branch_long", 4217,
        # not sure yet how to deal with second stop, but i think this works
        "70", "SOUTH", "georgia_irving", 19186,  # irving stop
        "70", "SOUTH", "georgia_irving", 10981,  # columbia stop
        "70", "SOUTH", "georgia_piney_branch_shrt", 4217,
        "S1", "NORTH", "sixteenth_u_shrt", 18042,
        "S2", "NORTH", "sixteenth_u_shrt", 18042,
        "S4", "NORTH", "sixteenth_u_shrt", 18042,
        "S9", "NORTH", "sixteenth_u_long", 18042,
        "64", "NORTH", "eleventh_i_new_york", 16490,
        "G8", "EAST", "eleventh_i_new_york", 16490,
        "D32", "EAST", "irving_fifteenth_sixteenth", 2368,
        "H1", "NORTH", "irving_fifteenth_sixteenth", 2368,
        "H2", "EAST", "irving_fifteenth_sixteenth", 2368,
        "H3", "EAST", "irving_fifteenth_sixteenth", 2368,
        "H4", "EAST", "irving_fifteenth_sixteenth", 2368,
        "H8", "EAST", "irving_fifteenth_sixteenth", 2368,
        "W47", "EAST", "irving_fifteenth_sixteenth", 2368
    )

    xwalk_wmata_route_dir_pattern = wr.read_sched_db_patterns(
        path=path_wmata_schedule_data,
        analysis_routes=analysis_routes) \
        [['direction', 'route', 'pattern']] \
        .drop_duplicates()

    xwalk_seg_pattern_stop = (xwalk_seg_pattern_stop_in
                              .merge(xwalk_wmata_route_dir_pattern, on=['route', 'direction'])
                              .reindex(columns=['route', 'pattern','direction' ,'seg_name_id', 'stop_id']))

    del xwalk_seg_pattern_stop_in

    # 2. load segment-pattern crosswalk (could be loaded separately or summarized from above)
    xwalk_seg_pattern = (xwalk_seg_pattern_stop.drop('stop_id', 1)
                         .drop_duplicates())
    return xwalk_seg_pattern



def combine_field_rawnav_dat(field_df, rawnav_stop_area_df, rawnav_summary_df, field_obs_time=['15:40', '18:10']):

    # Get the bus ID
    ################################################################
    rawnav_stop_area_q_jump_bus_id = (
        rawnav_stop_area_df.loc[lambda x: x.odom_ft == x.odom_ft_qj_stop, :]
            .filter(items=['filename', 'index_run_start', 'seg_name_id', 'route', 'pattern', 'start_date_time', 'wday',
                           'sec_past_st'])
            .merge(
            rawnav_summary_df.
                filter(items=['filename', 'index_run_start', 'file_busid', 'tag_busid']),
            on=['filename', 'index_run_start'],
            how='left'
        )
            .assign(sec_past_st=lambda x: pd.to_timedelta(x.sec_past_st, 'sec'),
                    start_date_time=lambda x: pd.to_datetime(x.start_date_time, infer_datetime_format=True),
                    qjump_date_time=lambda x: x.sec_past_st + x.start_date_time,
                    file_busid=lambda x: x.file_busid.astype(int),
                    tag_busid=lambda x: x.tag_busid.astype(int),
                    route=lambda x: x.route.astype(str))
    )

    # Get the dwell time
    ################################################################
    rawnav_stop_area_q_jump = (
        rawnav_stop_area_df.loc[lambda x: x.stop_area_phase == "t_stop1", :]
            .filter(items=['filename', 'index_run_start', 'secs_marg'])
            .rename(columns={'secs_marg': 'dwell_time'})
            .groupby(['filename','index_run_start']).dwell_time.sum().reset_index()
            .merge(
            rawnav_stop_area_q_jump_bus_id,
            on=['filename', 'index_run_start'],
            how='left'
        )
            .assign(dwell_time=lambda x: pd.to_timedelta(x.dwell_time, 'sec'))
            .set_index('qjump_date_time')
    )

    # Get the time from the stop zone start to crossing the intersection
    rawnav_stop_area_tot_time = (
        rawnav_stop_area_df.loc[lambda x: x.odom_ft >= x.odom_ft_qj_stop - 40, :] #40 ft. back from the queue jump
         .filter(items=['filename', 'index_run_start', 'sec_past_st'])
        .groupby(['filename', 'index_run_start'])
        .agg(tot_time_stop_to_150_ft= pd.NamedAgg("sec_past_st", lambda x:x.max()-x.min()))
        .reset_index()
        .assign(tot_time_stop_to_150_ft = lambda x: pd.to_timedelta(x.tot_time_stop_to_150_ft, 'sec'))
    )


    # Use runs that were during the field obs
    ################################################################
    rawnav_stop_area_q_jump_peak = (
        rawnav_stop_area_q_jump.between_time(field_obs_time[0],field_obs_time[1]).reset_index()
        .merge(rawnav_stop_area_tot_time,
               on = ['filename','index_run_start'],
               how = 'left')
    )


    field_rawnav_qjump = (
        field_df
            .merge(rawnav_stop_area_q_jump_peak,
                   left_on=['metrobus_route_field', 'bus_id_field'],
                   right_on=['route', 'file_busid'],
                   how='outer')
            .assign(diff_field_rawnav_approx_time=lambda x: (x.time_entered_stop_zone_field - x.qjump_date_time)
                    .apply(lambda x: x.total_seconds()),
                    diff_field_rawnav_dwell_time=lambda x: (x.dwell_time_field - x.dwell_time)
                    .apply(lambda x: x.total_seconds()),
                    diff_field_rawnav_tot_int_clear_time=lambda x: (x.total_time_at_intersection_field - x.tot_time_stop_to_150_ft)
                    .apply(lambda x: x.total_seconds()))
    )
    return field_rawnav_qjump

