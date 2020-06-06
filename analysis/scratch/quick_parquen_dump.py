# -*- coding: utf-8 -*-
"""
Created on Tue May 19 03:18:19 2020

@author: WylieTimmerman
"""
import pandas as pd, os, numpy as np, pyproj, sys, zipfile, glob, logging
import pyarrow as pa
import pyarrow.parquet as pq

path_sp = r"C:\OD\Foursquare ITP\Foursquare ITP SharePoint Site - Shared Documents\WMATA Queue Jump Analysis"
path_processed_data = os.path.join(path_sp,r"Client Shared Folder\data\02-processed")
path_interim_data = os.path.join(path_sp,r"Client Shared Folder\data\01-interim")

FinDat = pq.read_table(source =os.path.join(path_interim_data,"Route79_Partition_20200519.parquet")).to_pandas()

FinDat.to_csv(os.path.join(path_interim_data,"Route79_20200519.csv"))