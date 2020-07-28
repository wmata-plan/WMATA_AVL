# Process the field data

from IPython import get_ipython  # run magic commands
ipython = get_ipython()
ipython.magic("reset -f")
import numpy as np, pandas as pd, os, sys

#Globoals and custom function
path_source_data = r"C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents" \
                   r"\WMATA-AVL\Data\field_rawnav_dat"
path_to_repo = os.getcwd()
path_to_script_dir = os.path.join(path_to_repo, r'analysis\exploratory\03_Compare_rawnav_to_field_dir')
path_processed_data = os.path.join(path_source_data, "processed_data")
sys.path.append(path_to_script_dir)
from helper_function_field_validation import time_to_sec, correct_data_types, quick_and_dirty_schedule_qjump_mapping
from helper_function_field_validation import combine_field_rawnav_dat


path_field_dir = r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\WMATA-AVL\Data' \
                 r'\field_rawnav_dat'
path_field_file = os.path.join(path_field_dir, 'WMATA AVL field obsv spreadsheet v.2.xlsx')
field_xlsx_wb = pd.ExcelFile(path_field_file)
field_dict = {}

col_keep = ['Metrobus Route', 'Bus ID', "Today's Date", 'Signal Phase', 'Time Entered Stop Zone',
            'Time Left Stop Zone',
            'Front Door Open Time', 'Front Door Close Time', 'Rear Door Open Time', 'Rear Door Close Time',
            'Dwell Time',
            'Number of boardings', 'Number of alightings', 'Total Time at Intersection', 'Traffic Conditions',
            'Notes']

col_new_names = ['metrobus_route_field', 'bus_id_field', 'date_obs_field', 'signal_phase_field',
                 'time_entered_stop_zone_field','time_left_stop_zone_field',
                 'front_door_open_time_field', 'front_door_close_time_field', 'rear_door_open_time_field',
                 'rear_door_close_time_field','dwell_time_field','number_of_boarding_field',
                 'number_of_alightings_field', 'total_time_at_intersection_field', 'traffic_conditions_field',
                 'notes_field']
field_col_rename_map = {key: value for (key, value) in zip(col_keep, col_new_names)}

for sheet in field_xlsx_wb.sheet_names:
    dat = field_xlsx_wb.parse(sheet)
    dat = dat.loc[:, col_keep].rename(columns=field_col_rename_map)
    dat = correct_data_types(dat)
    field_dict[sheet] = dat

# Analyze data for 16th & U and Georgia & Columbia
########################################################################################################################
field_qjump_16_U = field_dict['16th & U']
field_qjump_georgia_columbia = field_dict['Georgia & Columbia']

path_stop_rawnav = os.path.join(path_processed_data,"rawnav_stop_areas")
rawnav_stop_area_16_U = pd.read_csv(os.path.join(path_stop_rawnav,'ourpoints_sixteenth_u_shrt.csv'),index_col=0)

path_summary_rawnav = os.path.join(path_processed_data,"rawnav_summary_check.xlsx")
rawnav_summary = pd.read_excel(path_summary_rawnav,index_col=0)

field_rawnav_qjump_16_U = combine_field_rawnav_dat(
    field_df =field_qjump_16_U,
    rawnav_stop_area_df =rawnav_stop_area_16_U,
    rawnav_summary_df =rawnav_summary,
    field_obs_time = ('15:50', '18:10')
)


field_qjump_georgia_columbia = field_dict['Georgia & Columbia']
path_stop_rawnav = os.path.join(path_processed_data,"rawnav_stop_areas")
rawnav_stop_area_georgia_columbia_1 = pd.read_csv(os.path.join(path_stop_rawnav,'ourpoints_georgia_irving.csv'),index_col=0)
rawnav_stop_area_georgia_columbia_2 = pd.read_csv(os.path.join(path_stop_rawnav,'ourpoints_georgia_columbia.csv'),index_col=0)
rawnav_stop_area_georgia_columbia = pd.concat([rawnav_stop_area_georgia_columbia_1,rawnav_stop_area_georgia_columbia_2])


field_rawnav_qjump_georgia_columbia = combine_field_rawnav_dat(
    field_df =field_qjump_georgia_columbia,
    rawnav_stop_area_df =rawnav_stop_area_georgia_columbia,
    rawnav_summary_df =rawnav_summary,
    field_obs_time = ('7:00', '9:10')
)

first_4_cols = ['metrobus_route_field','route','pattern','seg_name_id','bus_id_field','file_busid','tag_busid',
                'time_entered_stop_zone_field','qjump_date_time','diff_field_rawnav_approx_time','dwell_time_field',
                'dwell_time','diff_field_rawnav_dwell_time','total_time_at_intersection_field',
                'tot_time_stop_to_150_ft','diff_field_rawnav_tot_int_clear_time']
reindex_col = first_4_cols + [ col for col in field_rawnav_qjump_16_U.columns if col not in first_4_cols]

field_rawnav_combine = (pd.concat([field_rawnav_qjump_16_U, field_rawnav_qjump_georgia_columbia])
    .reset_index(drop=True)
    .reindex(reindex_col,axis=1)
)

path_validation_df = os.path.join(path_processed_data,"field_rawnav_dat.csv")
pd.DataFrame.to_csv(field_rawnav_combine,path_validation_df)