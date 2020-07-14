# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 22:24:39 2020

@author: WylieTimmerman
"""

nanfiles =(
    pq.read_table(source=os.path.join(path_rawnav_data ),
                  filters= [("route","=","nan")],
                  use_pandas_metadata = True)
    .to_pandas())

nanfiles_keys = (
    nanfiles[['filename','index_run_start']]
    .drop_duplicates()
    )

nanfiles_keys.to_csv("nanfiles_keys.csv")

nanfiles_index = nanfiles_keys.set_index(['filename','index_run_start'])

summary_rawnav_testing = summary_rawnav.copy(deep = True)

summary_rawnav_testing.set_index(['filename','index_run_start'], inplace = True)

summary_rawnav_testing_nan = summary_rawnav.merge(nanfiles_keys,
                                                  on = ['filename','index_run_start'],
                                                  how = 'right')

summary_rawnav_testing_nan.to_csv('nanfiles_summary.csv')

rawnav_inventory_filtered_valid.to_csv('rawnav filtered valid.csv')
