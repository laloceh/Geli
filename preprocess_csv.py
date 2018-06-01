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

inputfile = 'small_data.txt'
outputfile = 'small_data.xlsx'

df = pd.read_csv(inputfile)

final_df = pd.DataFrame(columns = ['val', 'points', 'year', 'month','week'])

#sorted(df.NAMELSAD.unique())

df['name_codes'] = df.NAMELSAD.astype('category').cat.codes

df = df.drop(['NAMELSAD','OBJECTID'],axis=1)
#print df.head()

cols = list(df.columns[:-1])

count = 1
for c in cols:
    if '.' not in c:
        print 'Processing:%s [%d] ' % (c,count)
        count = count + 1
        
        temp_df = df[[c,'name_codes']]
        
        year = fwn.get_year(c) 
        month = fwn.get_month(c)
        week = fwn.get_week_number(c)
        #print year, month, week
        
        temp_df.loc[:,'year'] = year
        temp_df.loc[:,'month'] = month
        temp_df.loc[:,'week'] = week
        
        ##change column names
        temp_df.rename(columns={c:'val', 'name_codes':'points'}, inplace=True)
        #print df_temp
        
        ## Add to the total DF
        final_df = pd.concat([final_df, temp_df])
    else:
        break
    
# change the column order
final_df = final_df[['points', 'year','month','week','val']]

final_df[['points','year','month','week']] = final_df[['points','year','month','week']].astype(int) 
# sort by points
final_df.sort_values(by=['points'], ascending=True ,inplace=True)

'''
    Write to Excel
'''
writer = ExcelWriter(outputfile)
final_df.to_excel(writer, 'Sheet1', index=False)
writer.save()

