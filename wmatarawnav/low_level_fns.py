# -*- coding: utf-8 -*-
"""
Created on Mon Jul  6 22:38:06 2020

@author: WylieTimmerman
"""
import pandas as pd 

def tribble(columns, *data):
    return pd.DataFrame(
        data=list(zip(*[iter(data)]*len(columns))),
        columns=columns
    )


def check_convert_list(possible_list):
    if isinstance(possible_list,str):
        return ([possible_list])
    else:
        return (possible_list)