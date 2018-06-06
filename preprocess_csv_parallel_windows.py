#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  6 15:28:55 2018

@author: eduardo
"""

"""
Created on Wed Jun  6 15:28:55 2018

@author: edgar ceh
"""


import multiprocessing
from multiprocessing import cpu_count
import sys
import pandas as pd
import open_file_window
import time
import os
import warnings
import find_week_number as fwn

warnings.filterwarnings('ignore')

CSV_CHUNKS = 10000                                      # Chunk size to be written to CSV
MSSQL_CHUNKS = 1000                                     # Chunk size to be written to MS SQL table

def worker(df_temp,c_range,df,final_df_list):
    '''
        Multiprocessor worker function
        For each columns in the list of columns call the processing function
    '''
    proc = os.getpid()                                  # Process ID (PID)
    i,e = c_range                                       # Obtain initial and end column range

                                                        # If it is only 1 column
    if i == e:
        col_list = list(df_temp.iloc[:,i].columns)
    else:
        col_list = list(df_temp.iloc[:,i:e].columns)

    final_df = pd.DataFrame()                           # Pandas DataFrame to store the result from each column
    for c in col_list:                                  # For each column
        temp_df = process_column(c,df)                  # Call the processing function
        final_df = pd.concat([final_df, temp_df])       # Concatenate the resulting DataFrame after processing

    print 'Processing %d rows' % final_df.shape[0]      # Print number of rows created
    final_df_list.append(final_df)                      # Append the DataFrame to a list of resulting DataFrames

def process_column(c,df):
    '''
        Function that make the columns decomposition
        Finds the year, month, and week number
        Creates a DataFrame for that column
    '''
    new_c = c.replace('F','')                           # Removes character 'F'
    new_c = new_c.replace('_EVI','')                    # Removes string '_EVI'

    temp_df = df[[c,'OBJECTID','NAMELSAD']]             # Creates a DataFrame for with the column name, ObjectID, and Name
    temp_df.rename(columns={c:'an'}, inplace=True)      # Change the column name for 'an'  
    year = fwn.get_year(new_c)                          # Get the year
    month = fwn.get_month(new_c)                        # Get the month
    week = fwn.get_week_number(new_c)                   # Get the week number
         
    temp_df.loc[:,'year'] = year                        # Insert a column with the year
    temp_df.loc[:,'month'] = month                      # Insert a column with the month
    temp_df.loc[:,'week'] = week                        # Insert a column with the week number
   
    return temp_df                                      # Returns the DataFrame for the column to worker

##################################################
############# M A I N ~ P R O G R A M ############
##################################################

if __name__ == "__main__":
    
    inputfile = open_file_window.open_files()           # Open the window to select the INPUT file (i.e. Export_Output_EVI.txt)
    if not inputfile:                                   # If nothing selected Exit
        print 'No input file selected'
        sys.exit(10)
    time.sleep(1)                                       # Wait 1 second
    
    outputfile_csv = open_file_window.save_to_files()   # Open the window to select the OUTPUT file (i.e. Export_Output_EVI-OUTPUT.txt)
    if not outputfile_csv:                              # If nothing selected Exit
        print 'No output file selected'
        sys.exit(10)

    df = pd.read_csv(inputfile)                         # Read the INPUT file into a DataFrame
    df_temp = df.copy()                                 # Create a copy of the DataFrame                                     
    df_temp.drop(['OBJECTID','NAMELSAD'], axis=1, inplace=True)  # Remove those columns to get only column with the dates
    
    cols = list(df_temp.columns)                        # Create a list with the column names
    total_cols = len(cols)                              # Count the total number of columns

    manager = multiprocessing.Manager()                 # Create a manager object for a shared List among all the processes
    final_df_list = manager.list()                      # Create the shared list
    jobs = []                                           # Create a list for all the processes
    proc = cpu_count()                                  # Obtain the number of CPU cores available for this computer
    #all_cols = total_cols                                
    ranges = proc                                       # The range of the columns to be passed to each process is equal the number of CPU cores
    print "Num. of processors %d" % ranges

    for i in range(0,total_cols,ranges):                # Create the column ranges
        c_range = (i,i+ranges)                          # Create the tuple initial_column, end_column)
        p = multiprocessing.Process(target=worker, args=(df_temp,c_range,df,final_df_list,))    # Create process, with the arguments to be used for each one
        jobs.append(p)                                  # Append the process to the list of processes created
        p.start()                                       # Start the process

    for p in jobs:                                      # Assign a process to a CPU core
        p.join()

    final_df = pd.DataFrame(columns = ['an', 'OBJECTID','NAMELSAD','year', 'month','week']) # Create a DataFrame to store all the results
    for df in final_df_list:                            # Concatenate each DataFrame created by each process into a final result
        final_df = pd.concat([final_df, df])

    final_df.rename(columns={'OBJECTID':'grid_fid'}, inplace=True)  # Change the column name 'OBJECTID' to 'grid_fid'
    final_df = final_df[['grid_fid','NAMELSAD', 'year','month','week','an']]    # Change the columns order 
    final_df[['grid_fid','year','month','week']] = final_df[['grid_fid','year','month','week']].astype(int) # Change the data type to have a smaller size DataFrame in memory
    final_df.sort_values(by=['grid_fid','year','month','week'], ascending=True ,inplace=True)     # Sort the entire DataFrame according to the columns 'grid_fid','year','month','week'
    print
    print 'Final DF shape',final_df.shape               # Print the DataFrame final shape

    
    '''
        Write to CSV
    '''
    print "Writing to CSV"
    final_df.to_csv(outputfile_csv,index=False, chunksize=CSV_CHUNKS)   # Export the final DataFrame to a CSV file, using the OUPUT file
    print "File %s created" % outputfile_csv
    
