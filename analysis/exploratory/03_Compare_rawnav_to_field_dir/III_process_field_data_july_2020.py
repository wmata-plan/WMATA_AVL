import pandas as pd
import os
import plotly.express as px
import plotly.offline as pyo
from plotly.offline import plot
# Set notebook mode to work in offline
pyo.init_notebook_mode()
# need for regression trendline
import statsmodels
# Globoals and custom function
import wmatarawnav as wr
from helper_function_field_validation import correct_data_types
from helper_function_field_validation import combine_field_rawnav_dat
from helper_function_field_validation \
    import quick_and_dirty_schedule_qjump_mapping
# Globoals and custom function
path_source_data = (
    r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents" 
    r"\WMATA-AVL\Data\field_rawnav_dat")
path_processed_data = os.path.join(path_source_data, "processed_data")
path_field_dir = (
    r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents'
    r'\WMATA-AVL\Data\field_rawnav_dat')
path_field_file = os.path.join(
    path_field_dir, 'WMATA AVL field obsv spreadsheet v.2-axb.xlsx')
path_wmata_schedule_data = (r"C:\Users\abibeka\OneDrive - "
                            r"Kittelson & Associates, Inc\Documents"
                            r"\WMATA-AVL\Data\wmata_schedule_data"
                            r"\Schedule_082719-201718.mdb")
field_xlsx_wb = pd.ExcelFile(path_field_file)
field_dict = {}

col_keep = [
    'Metrobus Route', 'Bus ID', "Today's Date", 'Signal Phase',
    'Time Entered Stop Zone', 'Time Left Stop Zone', 'Front Door Open Time',
    'Front Door Close Time', 'Rear Door Open Time', 'Rear Door Close Time',
    'Dwell Time', 'Number of boardings', 'Number of alightings',
    'Total Time at Intersection', 'Traffic Conditions', 'Notes',
]
col_new_names = [
    'metrobus_route_field', 'bus_id_field', 'date_obs_field',
    'signal_phase_field', 'time_entered_stop_zone_field',
    'time_left_stop_zone_field', 'front_door_open_time_field',
    'front_door_close_time_field', 'rear_door_open_time_field',
    'rear_door_close_time_field', 'dwell_time_field',
    'number_of_boarding_field', 'number_of_alightings_field',
    'total_time_at_intersection_field', 'traffic_conditions_field',
    'notes_field',
]
field_col_rename_map = {
    key: value for (key, value) in zip(col_keep, col_new_names)
}

q_jump_field_loc = [
    "16th & U", "Georgia & Columbia", "Georgia & Irving",
    "Georgia & Piney Branch", "11th & NY", "Irving & 16th"
]

rawnav_qjump_nm_map = {
    "georgia_columbia": "Georgia & Columbia",
    "georgia_piney_branch_long": "Georgia & Piney Branch",
    "georgia_piney_branch_shrt": "Georgia & Piney Branch",
    "georgia_irving": "Georgia & Irving",
    "sixteenth_u_shrt": "16th & U",
    "sixteenth_u_long": "16th & U",
    "eleventh_i_new_york": "11th & NY",
    "irving_fifteenth_sixteenth": "Irving & 16th",
}

for sheet in q_jump_field_loc:
    dat = field_xlsx_wb.parse(sheet)
    dat = dat.loc[:, col_keep].rename(columns=field_col_rename_map)
    dat = dat.query('~bus_id_field.isna()')
    dat = correct_data_types(dat)
    dat.loc[:, "field_qjump_loc"] = sheet
    field_dict[sheet] = dat

field_df_all_loc = pd.concat(field_dict.values())
# Analyze data for 16th & U and Georgia & Columbia
###############################################################################
path_stop_rawnav = os.path.join(path_processed_data, "rawnav_stop_areas")
rawnav_tt_decomp = pd.read_csv(os.path.join(path_stop_rawnav,
                                            'traveltime_decomp.csv'),
                               index_col=0)
rawnav_tt_decomp = (
    rawnav_tt_decomp
    .assign(field_qjump_loc=(lambda df: df.seg_name_id.replace(
                rawnav_qjump_nm_map)))
    .drop_duplicates(["filename", "index_run_start"])
)

analysis_routes = [
    'S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3',
    'H4', 'H8', 'W47'
]
analysis_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday',
                 'Saturday', 'Sunday']
rawnav_summary = wr.read_cleaned_rawnav(
                analysis_routes_=analysis_routes,
                analysis_days_=analysis_days,
                path=os.path.join(path_processed_data,
                                  "rawnav_summary.parquet"))

xwalk_seg_pattern_stop = quick_and_dirty_schedule_qjump_mapping()

rawnav_summary_fil = (
    rawnav_summary
    .query("dist_odom_mi >= 1")
    .merge(
        xwalk_seg_pattern_stop,
        on=["route", "pattern"],
        how="right"
    )
    .query("~ start_date_time.isna()")
    .assign(qjump_approx_date_time=(lambda df: df.start_date_time
                                    + df.approx_time_qjump),
            field_qjump_loc=(lambda df: df.seg_name_id.replace(
                rawnav_qjump_nm_map)),
            file_busid=lambda df: df.file_busid.astype(int)
            )
)


field_q_jump_rawnav_list = []
for q_jump_field_loc in field_dict.keys():
    field_q_jump_1loc = field_dict[q_jump_field_loc]
    field_dates = field_q_jump_1loc.date_obs_field.dt.date.unique()
    for field_date in field_dates:
        field_q_jump_1loc_1day = (
            field_q_jump_1loc
            .loc[lambda x:x.date_obs_field.dt.date == field_date]
        )
        q_jump_field_loc_this_stop \
            = field_q_jump_1loc_1day.field_qjump_loc.unique()[0]
        min_time = (field_q_jump_1loc_1day.time_entered_stop_zone_field.min()
                    - pd.to_timedelta(1000, "s")).time()
        max_time = (field_q_jump_1loc_1day.time_entered_stop_zone_field.max()
                    + pd.to_timedelta(1000, "s")).time()

        rawnav_summary_fil_this_stop = (
            rawnav_summary_fil
            .loc[lambda x: ((x.field_qjump_loc == q_jump_field_loc_this_stop)
                            & (x.start_date_time.dt.date == field_date))]
            .set_index("qjump_approx_date_time")
            .between_time(min_time, max_time)
            .reset_index()
            .drop(columns=["field_qjump_loc"])
        )

        field_q_jump_1loc_1day_rawnav = (
            field_q_jump_1loc_1day
            .assign(
                metrobus_route_field=(lambda df:
                                      df.metrobus_route_field
                                      .astype("str")
                                      .apply(lambda x: x.split(".")[0])),
                bus_id_field=(lambda df: df.bus_id_field.astype("int"))
            )
            .merge(rawnav_summary_fil_this_stop
                   .assign(route=(
                        lambda df: df.route.astype("str")),
                        file_busid=(
                        lambda df: df.file_busid.astype("int"))
                    ),
                   left_on=["metrobus_route_field", "bus_id_field"],
                   right_on=["route", "file_busid"],
                   how="left"
                   )
            .eval("approx_diff_field_rawnav_time=(qjump_approx_date_time"
                  "- time_entered_stop_zone_field)"
                  ".dt.total_seconds()")
            .eval("check_diff_field_rawanv_start_time=("
                  "time_entered_stop_zone_field"
                  "- start_date_time)"
                  ".dt.total_seconds()")
            .reindex(columns=[
                "metrobus_route_field", "bus_id_field",
                "route","pattern", "file_busid",
                "qjump_approx_date_time",
                "field_qjump_loc",
                "time_entered_stop_zone_field",
                "filename", "index_run_start",
                "run_duration_from_sec", "dist_odom_mi", "mph_run_tag",
                "dist_crow_fly_mi", "start_date_time",
                "approx_diff_field_rawnav_time",
                "check_diff_field_rawanv_start_time"])
        )
        field_q_jump_rawnav_list.append(field_q_jump_1loc_1day_rawnav)

field_q_jump_rawnav_df = pd.concat(field_q_jump_rawnav_list)

field_q_jump_rawnav_df_fil = (
    field_q_jump_rawnav_df
        .loc[
        lambda x: (x.check_diff_field_rawanv_start_time > 0)
                  & (x.approx_diff_field_rawnav_time.abs() < 800),
    ]
)

field_q_jump_rawnav_df_all = (
    field_df_all_loc
    .assign(
    metrobus_route_field=(lambda df: df.metrobus_route_field
                          .astype("str").apply(lambda x: x.split(".")[0])),
    bus_id_field=(lambda df: df.bus_id_field.astype("int"))
    )
    .merge(
        field_q_jump_rawnav_df_fil,
        on=["field_qjump_loc", "metrobus_route_field", "bus_id_field",
            "time_entered_stop_zone_field"],
        how="left"
    )
)

first_cols = [
    'field_qjump_loc', 'metrobus_route_field', 'route', 'pattern',
    'bus_id_field', 'file_busid', 'approx_diff_field_rawnav_time',
    'time_entered_stop_zone_field', 'qjump_approx_date_time',
]
reindex_col = (first_cols
               + [col for col in field_q_jump_rawnav_df_all.columns
                  if col not in first_cols]
               )

field_q_jump_rawnav_df_all = (field_q_jump_rawnav_df_all
                              .reindex(reindex_col, axis=1)
                              )

path_validation_df = os.path.join(path_processed_data, "field_rawnav_dat.csv")
pd.DataFrame.to_csv(field_q_jump_rawnav_df_all, path_validation_df)


field_q_jump_rawnav_dwell_t_cntr_list = []
for q_jump_field_loc in field_dict.keys():
    field_q_jump_1loc = field_dict[q_jump_field_loc]
    field_dates = field_q_jump_1loc.date_obs_field.dt.date.unique()
    for field_date in field_dates:
        field_q_jump_1loc_1day = (
            field_q_jump_1loc
            .loc[lambda x:x.date_obs_field.dt.date == field_date]
        )
        q_jump_field_loc_this_stop \
            = field_q_jump_1loc_1day.field_qjump_loc.unique()[0]
        min_time = (field_q_jump_1loc_1day.time_entered_stop_zone_field.min()
                    - pd.to_timedelta(500, "s")).time()
        max_time = (field_q_jump_1loc_1day.time_entered_stop_zone_field.max()
                    + pd.to_timedelta(500, "s")).time()

        field_q_jump_rawnav_dwell_t_cntr_list.append(
            combine_field_rawnav_dat(
                field_df=field_q_jump_1loc_1day,
                rawnav_stop_area_df=rawnav_tt_decomp,
                rawnav_summary_df=rawnav_summary_fil,
                field_qjump_loc=q_jump_field_loc,
                field_obs_time=(min_time, max_time),
                field_obs_date=field_date
            )
        )

field_q_jump_rawnav_dwell_t_cntr = \
    pd.concat(field_q_jump_rawnav_dwell_t_cntr_list)


first_cols_1 = [
    'field_qjump_loc', 'seg_name_id', 'metrobus_route_field', 'route',
    'pattern', 'bus_id_field', 'file_busid', 'time_entered_stop_zone_field',
    'qjump_date_time', 'diff_field_rawnav_approx_time', 'dwell_time_field',
    'dwell_time', 'diff_field_rawnav_dwell_time',
    'total_time_at_intersection_field', 'tot_time_stop_to_150_ft',
    'diff_field_rawnav_tot_int_clear_time'
]
reindex_col = (first_cols_1
               + [col for col in field_q_jump_rawnav_dwell_t_cntr.columns
                  if col not in first_cols_1]
               )

field_q_jump_rawnav_dwell_t_cntr = (
    field_q_jump_rawnav_dwell_t_cntr
    .reindex(reindex_col, axis=1)
)

path_validation_df_1 = os.path.join(path_processed_data,
                                    "field_processed_rawnav_dat.csv")
pd.DataFrame.to_csv(field_q_jump_rawnav_dwell_t_cntr, path_validation_df_1)



field_rawnav_combine_dwell_plt = (
    field_q_jump_rawnav_dwell_t_cntr
    .assign(
        dwell_time_field=lambda df: (df.dwell_time_field
                                      .apply(lambda df1: df1.total_seconds())
                                     ),
        dwell_time=lambda df: (df.dwell_time
                               .apply(lambda df1: df1.total_seconds())),
        route_seg_name_id=lambda x: x.route+", "+x.seg_name_id)
)
p1 = px.scatter(
    data_frame=field_rawnav_combine_dwell_plt.query('route==route'),
    x="dwell_time_field",
    y="dwell_time",
    symbol = "route_seg_name_id",
    color="route_seg_name_id",
    hover_data=["diff_field_rawnav_dwell_time"])
plot(p1, filename=os.path.join(path_processed_data,
                               "dwell_time_comb.html"))
p2 = px.scatter(
    data_frame=field_rawnav_combine_dwell_plt.query('~ route.isna()'),
    x="dwell_time_field",
    y="dwell_time",
    hover_data=["diff_field_rawnav_dwell_time"],
    trendline="ols")
plot(p2, filename=os.path.join(path_processed_data,
                               "dwell_time_trendline_comb.html"))

field_rawnav_combine_clear_time_plt = (
    field_q_jump_rawnav_dwell_t_cntr
    .assign(
        t_control_delay_field=(lambda df: df.t_control_delay_field
                               .apply(lambda df1: df1.total_seconds())),
        t_traffic=(lambda x: x.t_traffic.apply(lambda x:x.total_seconds())),
        route_seg_name_id=lambda x: x.route+", "+x.seg_name_id)
)

g1 = px.scatter(
    data_frame=field_rawnav_combine_clear_time_plt.query('~ route.isna()'),
    y="t_control_delay_field",
    x="t_traffic",
    symbol="route_seg_name_id",
    color="route_seg_name_id",
    hover_data=[
        "diff_field_rawnav_control_delay", "t_ff", "t_segment",
        "traffic_conditions_field", "notes_field"])
plot(g1, filename=os.path.join(
    path_processed_data,
    "approx_fieldcontrol_delay_vs_q_jump_seg_traffic_delay.html"))


g2 = px.scatter(
    data_frame=field_rawnav_combine_clear_time_plt.query('~ route.isna()'),
    y="t_control_delay_field",
    x="t_traffic",
    hover_data=[
        "diff_field_rawnav_control_delay", "t_ff", "t_segment",
        "traffic_conditions_field", "notes_field"],
    trendline="ols")
plot(g2, filename=os.path.join(
    path_processed_data,
    "approx_trendline_fieldcontrol_delay_vs_q_jump_seg_traffic_delay.html"))


