# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 06:16:00 2020

@author: WylieTimmerman
"""

query_string = 'filename == "rawnav02833191007.txt" & index_run_start == 419'

stop_index_filt = (stop_index.query())

stop_index_filt

stop_index_filt_line = wr.make_target_rawnav_linestring(stop_index_filt)

rawnav_qjump_dat_filt = rawnav_qjump_dat.query('filename == "rawnav02833191007.txt" & index_run_start == 419')

wr.plot_rawnav_trajectory_with_wmata_schedule_stops(rawnav_qjump_dat_filt, stop_index_filt_line)

stop_summary_filt = stop_summary.query(query_string)



summary_stop_fil =  wmata_schedule_based_sum_dat_.query(query_string)

summary_fil = summary_rawnav.query(query_string)
