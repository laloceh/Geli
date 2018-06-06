import multiprocessing
from multiprocessing import cpu_count
import sys
import pandas as pd
import open_file_window
import time


def worker(df_temp,c,df):
    print c
    i,e = c

    if i == e:
        col_list = list(df_temp.iloc[:,i].columns)
    else:
        col_list = list(df_temp.iloc[:,i:e].columns)

    print col_list
    
    for c in col_list:
        process_column(c,df)
    return

def process_column(c,df):
    print c
    print df['account']

if __name__ == "__main__":
    
    '''
    Find the input file
    This is the file with the SIG exported data (.txt)
    '''
    inputfile = open_file_window.open_files()
    #inputfile = 'Export_Output_EVI.txt'
    if not inputfile:
        print 'No input file selected'
        sys.exit(10)
    time.sleep(1)
    '''
    Find the output file
    This is the file where to put the resulting .csv file
    '''
    outputfile_csv = open_file_window.save_to_files()
    if not outputfile_csv:
        print 'No output file selected'
        sys.exit(10)

    df = pd.read_csv(inputfile)
    df_temp = df.copy()
    df_temp.drop(['OBJECTID','NAMELSAD'], axis=1, inplace=True)
    
    cols = list(df_temp.columns)
    total_cols = len(cols)

    print total_cols
    print df_temp.head(3)
     
    jobs = []
    proc = cpu_count()
    todo = total_cols
    ranges = proc
    print ranges
    sys.exit(99)
    for i in range(0,todo,ranges):
        print i,i+ranges
        c = (i,i+ranges)
        p = multiprocessing.Process(target=worker, args=(df_temp,c,df,))
        jobs.append(p)
        p.start()

    sys.exit(99)
    for i in range(total_cols):
        c = (i,i+ranges)
        p = multiprocessing.Process(target=worker, args=(df_temp,c,))
        jobs.append(p)
        p.start()
