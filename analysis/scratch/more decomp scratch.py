# -*- coding: utf-8 -*-
"""
Created on Thu Jul 23 07:37:33 2020

@author: WylieTimmerman
"""

test = rawnav_fil_stop_area_3

test_2 = test.loc[test.any_veh_stop == False, ]

test_2 = test.loc[((test.any_veh_stop == True) & (test.door_state_changes_min.isnull())), ]

#case with early fps-next is nna
#rawnav00008191007.txt
#12334

# no stop
rawnav02899191026.txt
13021

# something weird
rawnav06160191012.txt
16577

test_3 = test.loc[(test.filename == "rawnav00008191007.txt") & (test.index_run_start == 12334), ]

test_4 = rawnav_fil_stop_area_4.loc[(rawnav_fil_stop_area_4.filename == "rawnav00008191007.txt") & (rawnav_fil_stop_area_4.index_run_start == 12334), ]


            # to preserve missing data where speed undefined, we make this column a 
            # float so we can use n
            veh_state_moving=lambda x: np.where(x.fps_next.isnull(), 
                                                np.nan,
                                                np.where(x.fps_next > 0,
                                                         1.0,
                                                         0.0)
                                                )  
rawnav_inventory_filtered =\
    rawnav_inventory[rawnav_inventory.groupby('filename',sort = False)['route'].transform(lambda x: x.isin(analysis_routes).any())]
            
            
test_5 = rawnav_fil_stop_area_4.loc[(rawnav_fil_stop_area_4.filename == "rawnav02899191026.txt") & (rawnav_fil_stop_area_4.index_run_start == 13021), ]

# not moving at all
test_6 = rawnav_fil_stop_area_3[(rawnav_fil_stop_area_3.groupby(['filename','index_run_start'])['any_veh_stopped'].transform(lambda x: ~x.all()))]

test_6 = rawnav_fil_stop_area_4.loc[rawnav_fil_stop_area_4.any_veh_stopped == False]

# soemthing 
test_7 = rawnav_fil_stop_area_4.loc[(rawnav_fil_stop_area_4.any_veh_stopped == False) & (rawnav_fil_stop_area_4.any_door_open == True)]

test_8_1 = rawnav_fil_stop_area_4.loc[(rawnav_fil_stop_area_4.filename == "rawnav06160191012.txt") & (rawnav_fil_stop_area_4.index_run_start == 16577), ]



# another 
test_10 = rawnav_fil_stop_area_5.loc[(rawnav_fil_stop_area_5.any_veh_stopped == False)]

test_10 = rawnav_fil_stop_area_5.loc[(rawnav_fil_stop_area_5.stop_area_phase == "t_stop")]

test_11 = rawnav_fil_stop_area_5[(rawnav_fil_stop_area_5.groupby(['filename','index_run_start'])['stop_area_phase'].transform(lambda x: x.isin(["t_stop"]).any())]

                                                      # TODO: fix this null check, is a hack for NAs in any_veh_stopped
                                                      & (x.any_veh_stopped == True | 
                                                         (x.any_veh_stopped.isnull() & (x.fps_next.isnull())))),
                                                     x.rough_phase_by_veh_state,
                                                     x.stop_area_phase)