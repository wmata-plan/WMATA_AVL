# -*- coding: utf-8 -*-
"""
Create by: abibeka
Purpose: Read WMATA schedule data; Schedule_082719-201718.mdb
"""
# https://stackoverflow.com/questions/39835770/read-data-from-pyodbc-to-pandas
import pandas as pd, os, inflection, numpy as np

mdb_to_excel_file_loc = r'C:\Users\abibeka\OneDrive - Kittelson & Associates,' \
                        r' Inc\Documents\WMATA-AVL\Data\wmata_schedule_data'
stop_file = os.path.join(mdb_to_excel_file_loc, 'Stop.xlsx')
stop_dat = pd.read_excel(stop_file)
stop_dat = stop_dat.dropna(axis=1)
stop_dat.columns = [inflection.underscore(col_nm) for col_nm in stop_dat.columns]

pattern_file = os.path.join(mdb_to_excel_file_loc, 'Pattern.xlsx')
pattern_dat = pd.read_excel(pattern_file)
pattern_dat = pattern_dat[['PatternID','TARoute','PatternName','Direction',
                           'Distance','CDRoute','CDVariation','PatternDestination',
                           'RouteText','RouteKey','PatternDestination2','RouteText2',
                           'Direction2','PatternName2','TARoute2','PubRouteDir','PatternNotes',
                           'DirectionID']]
#pattern_dat = pattern_dat.dropna(axis=1)
pattern_dat.columns = [inflection.underscore(col_nm) for col_nm in pattern_dat.columns]
pattern_dat.rename(columns={'distance':'trip_length'},inplace=True)

pattern_detail_file = os.path.join(mdb_to_excel_file_loc, 'PatternDetail.xlsx')
pattern_detail_dat = pd.read_excel(pattern_detail_file)
pattern_detail_dat = pattern_detail_dat.dropna(axis=1)
pattern_detail_dat = pattern_detail_dat.drop(columns=['SortOrder', 'GeoPathID'])
pattern_detail_dat.columns = [inflection.underscore(col_nm) for col_nm in pattern_detail_dat.columns]
pattern_detail_dat.rename(columns={'distance':'stop_dist'},inplace=True)

q_jump_route_list = ['S1', 'S2', 'S4', 'S9', '70', '79', '64', 'G8', 'D32', 'H1', 'H2', 'H3', 'H4', 'H8', 'W47']
pattern_q_jump_route_dat = pattern_dat.query('cd_route in @q_jump_route_list')
set(pattern_q_jump_route_dat.cd_route.unique()) - set(q_jump_route_list)

pattern_pattern_detail_stop_q_jump_route_dat = \
    pattern_q_jump_route_dat.merge(pattern_detail_dat,on='pattern_id',how='left')\
    .merge(stop_dat,on='geo_id',how='left')

pattern_pattern_detail_stop_q_jump_route_dat.\
    sort_values(by=['cd_route','cd_variation','order'],inplace=True)

mask_nan_latlong = pattern_pattern_detail_stop_q_jump_route_dat[['latitude', 'longitude']].isna().all(axis=1)
assert_stop_sort_order_zero_has_nan_latlong = \
    sum(pattern_pattern_detail_stop_q_jump_route_dat[mask_nan_latlong].stop_sort_order-0)
assert(assert_stop_sort_order_zero_has_nan_latlong==0)

no_nan_pattern_pattern_detail_stop_q_jump_route_dat =\
    pattern_pattern_detail_stop_q_jump_route_dat[~mask_nan_latlong]

no_nan_pattern_pattern_detail_stop_q_jump_route_dat = \
    no_nan_pattern_pattern_detail_stop_q_jump_route_dat.dropna(axis=1)

assert(0== sum(~ no_nan_pattern_pattern_detail_stop_q_jump_route_dat.
               eval('''direction==pub_route_dir& cd_route==ta_route''')))
no_nan_pattern_pattern_detail_stop_q_jump_route_dat.drop(columns=['pub_route_dir','ta_route'],inplace=True)

assert(0== np.sum(no_nan_pattern_pattern_detail_stop_q_jump_route_dat.isna().values))

save_file_path = r'C:\Users\abibeka\OneDrive - Kittelson & Associates, Inc\Documents\Github\WMATA_AVL\data\02-processed'
save_file = os.path.join(save_file_path,'wmata_schedule_data_q_jump_routes.csv')
no_nan_pattern_pattern_detail_stop_q_jump_route_dat.to_csv(save_file)

no_nan_pattern_pattern_detail_stop_q_jump_route_dat.iloc[0]