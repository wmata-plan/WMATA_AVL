# -*- coding: utf-8 -*-
"""
Created on Fri May 15 00:35:45 2020

@author: WylieTimmerman
"""

# Run 
wanted_keys = ['rawnav06437191005.txt']

RouteRawTagDictShort = \
    dict((k, RouteRawTagDict[k]) for k in wanted_keys if k in RouteRawTagDict)
   
RawnavDataDictTest = {}
SummaryDataDictTest = {}
for key, datadict in RouteRawTagDictShort.items():
    #CleanDataDict[key] = wr.clean_rawnav_data(datadict)
    Temp2 = wr.clean_rawnav_data(datadict, key)
    RawnavDataDictTest[key] = Temp2['rawnavdata']
    SummaryDataDictTest[key] = Temp2['SummaryData']
    
# now this errors with 'ValueError: The column label 'IndexTripEnd' is not unique.'
    
# failed with 347 in RawNavDataDict, so assuming error is at the 348th case
list(RouteRawTagDict.keys())[348]

wanted_keys = [list(RouteRawTagDict.keys())[348]]

RouteRawTagDictShort = \
    dict((k, RouteRawTagDict[k]) for k in wanted_keys if k in RouteRawTagDict)
   
RawnavDataDictTest2 = {}
SummaryDataDictTest2 = {}

for key, datadict in RouteRawTagDictShort.items():
    #CleanDataDict[key] = wr.clean_rawnav_data(datadict)
    Temp2 = wr.clean_rawnav_data(datadict, key)
    RawnavDataDictTest2[key] = Temp2['rawnavdata']
    SummaryDataDictTest2[key] = Temp2['SummaryData']
    

# this works first time, but I can't rerun it....
wanted_keys = [list(RouteRawTagDict.keys())[349]]

RouteRawTagDictShort = \
    dict((k, RouteRawTagDict[k]) for k in wanted_keys if k in RouteRawTagDict)
   
RawnavDataDictTest = {}
SummaryDataDictTest = {}

for key, datadict in RouteRawTagDictShort.items():
    #CleanDataDict[key] = wr.clean_rawnav_data(datadict)
    Temp2 = wr.clean_rawnav_data(datadict, key)
    RawnavDataDictTest[key] = Temp2['rawnavdata']
    SummaryDataDictTest[key] = Temp2['SummaryData']
    
wanted_keys = [list(RouteRawTagDict.keys())[350]]

RouteRawTagDictShort = \
    dict((k, RouteRawTagDict[k]) for k in wanted_keys if k in RouteRawTagDict)
   
RawnavDataDictTest = {}
SummaryDataDictTest = {}

for key, datadict in RouteRawTagDictShort.items():
    #CleanDataDict[key] = wr.clean_rawnav_data(datadict)
    Temp2 = wr.clean_rawnav_data(datadict, key)
    RawnavDataDictTest[key] = Temp2['rawnavdata']
    SummaryDataDictTest[key] = Temp2['SummaryData']