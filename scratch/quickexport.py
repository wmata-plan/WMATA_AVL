# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 03:49:51 2020

@author: WylieTimmerman
"""

df2616191003 = RawNavDataDict['79'][2616191003]


OutFi2 = os.path.join(path_processed_data,'rawnavexports.xlsx')
writer = pd.ExcelWriter(OutFi2)
df2616191003.to_excel(writer,'2616191003')

writer.save()
