#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  1 12:09:40 2018

@author: eduardo
"""
#https://pythonspot.com/write-excel-with-pandas/

import find_week_number as fwn
import pandas as  pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import sys

inputfile = 'Export_Output_EVI.txt'
#inputfile = 'small_data.txt'
#outputfile = 'small_data.xlsx'
outputfile = 'Export_Output_EVI.xlsx'

df = pd.read_csv(inputfile)

final_df = pd.DataFrame(columns = ['an', 'grid_fid', 'NAMELSAD', 'year', 'month','week'])

#sorted(df.NAMELSAD.unique())

df['name_codes'] = df.NAMELSAD.astype('category').cat.codes

#df = df.drop(['NAMELSAD','OBJECTID'],axis=1)
df = df.drop(['OBJECTID'],axis=1)
#print df.head()

cols = list(df.columns[1:-1])



count = 1
for c in cols:
    
    new_c = c.replace('F','')
    new_c = new_c.replace('_EVI','')
    
    
    if '.' not in c:
        print 'Processing:%s [%d] ' % (new_c,count)
        count = count + 1
        
        temp_df = df[[c,'name_codes','NAMELSAD']]
        temp_df.rename(columns={c:new_c}, inplace=True)
        
        year = fwn.get_year(new_c) 
        month = fwn.get_month(new_c)
        week = fwn.get_week_number(new_c)
        #print year, month, week
        
        temp_df.loc[:,'year'] = year
        temp_df.loc[:,'month'] = month
        temp_df.loc[:,'week'] = week
        
        ##change column names
        temp_df.rename(columns={c:'an', 'name_codes':'grid_fid'}, inplace=True)
        #print df_temp
        ## Add to the total DF
        final_df = pd.concat([final_df, temp_df])
        
    else:
        break


# change the column order
final_df = final_df[['grid_fid','NAMELSAD', 'year','month','week','an']]

final_df[['grid_fid','year','month','week']] = final_df[['grid_fid','year','month','week']].astype(int) 
# sort by points
final_df.sort_values(by=['grid_fid'], ascending=True ,inplace=True)

'''
    Write to Excel
'''
writer = ExcelWriter(outputfile)
final_df.to_excel(writer, 'Sheet1', index=False)
writer.save()

