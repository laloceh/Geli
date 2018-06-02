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
from multiprocessing import Pool, cpu_count
import warnings

warnings.filterwarnings('ignore')

COUNT = 1 

def process_Pandas_data(func, df, num_processes=None):
    ''' Apply a function separately to each column in a dataframe, in parallel.'''
    
    # If num_processes is not specified, default to minimum(#columns, #machine-cores)
    if num_processes==None:
        num_processes = min(df.shape[1], cpu_count())

    p = Pool(num_processes)
    ret_list = p.map(func, [col_name for col_name in df.columns])
    
    #print
    #print 'RET_LIST'
    #print ret_list
    
    p.close()
    p.join()
    return pd.concat(ret_list)


def process_data(c):
    global COUNT
    #print c
    
    #final_df = pd.DataFrame(columns = ['an', 'grid_fid', 'NAMELSAD', 'year', 'month','week'])
    #final_df = pd.DataFrame(columns = ['an', 'NAMELSAD', 'year', 'month','week'])
    final_df = pd.DataFrame(columns = ['an', 'OBJECTID','NAMELSAD', 'year', 'month','week'])
    
    #count = 1
        
    new_c = c.replace('F','')
    new_c = new_c.replace('_EVI','')
    
    
    if '.' not in c:
        print 'Processing:%s' % (new_c)
        #count = count + 1
        #COUNT = COUNT + 1
        
        #temp_df = df[[c,'name_codes','NAMELSAD']]
        temp_df = df[[c,'OBJECTID','NAMELSAD']]        
        temp_df.rename(columns={c:new_c}, inplace=True)
        
        year = fwn.get_year(new_c) 
        month = fwn.get_month(new_c)
        week = fwn.get_week_number(new_c)
        #print year, month, week
        
        temp_df.loc[:,'year'] = year
        temp_df.loc[:,'month'] = month
        temp_df.loc[:,'week'] = week
        
        ##change column names
        #temp_df.rename(columns={c:'an', 'name_codes':'grid_fid'}, inplace=True)
        temp_df.rename(columns={new_c:'an'}, inplace=True)
        #print temp_df
        
        ## Add to the total DF
        final_df = pd.concat([final_df, temp_df])
        
    return final_df

inputfile = 'Export_Output_EVI.txt'
outputfile = 'Export_Output_EVI.xlsx'
outputfile_csv = 'Export_Output_EVI.csv'

#inputfile = 'very_small_data.txt'
#outputfile = 'very_small_data.xlsx'

chunksize = 10000

final_list = []
# load the big file in smaller chunks
for df in pd.read_csv(inputfile, chunksize=chunksize):
    print 'CHUNK', df.shape
    
    #final_df = pd.DataFrame(columns = ['an', 'grid_fid', 'NAMELSAD', 'year', 'month','week'])
    #sorted(df.NAMELSAD.unique())
    
    ##df['name_codes'] = df.NAMELSAD.astype('category').cat.codes
    
    ##df = df.drop(['OBJECTID'],axis=1)
    #print df.head()
    
    cols = list(df.columns[2:])
    #print cols
    #print len(cols)/cpu_count() + 1
    dates_df = df[cols]
    #dates_df.iloc[:,0:5]
    proc = cpu_count()
    final_list.append(process_Pandas_data(process_data, dates_df, proc))

final_df = pd.concat(final_list)

##final_df['grid_fid'] = final_df.NAMELSAD.astype('category').cat.codes
final_df.rename(columns={'OBJECTID':'grid_fid'}, inplace=True)
final_df = final_df[['grid_fid','NAMELSAD', 'year','month','week','an']]
final_df[['grid_fid','year','month','week']] = final_df[['grid_fid','year','month','week']].astype(int) 
final_df.sort_values(by=['grid_fid'], ascending=True ,inplace=True)

print
print 'Final DF shape',final_df.shape    
'''
    Write to Excel
'''
#print "Writing to Excel"
#writer = ExcelWriter(outputfile)
#final_df.to_excel(writer, 'Sheet1', index=False)
#writer.save()
#print "File %s created" % outputfile

'''
    Write to CSV
'''
print "Writing to CSV"
final_df.to_csv(outputfile_csv,index=False, chunksize=chunksize)
print "File %s created" % outputfile_csv