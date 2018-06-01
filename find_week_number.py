#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu May 31 15:51:43 2018

@author: eduardo
"""

'''
    Given a day of the year and a year, return the week number
'''
from datetime import date
import sys
import pandas as pd

def return_full_date(day, year):
    '''
    Returns full date with format yyy-mm-dd
    Input: int day, int year
    Output: datetime.date
    '''
    
    return date.fromordinal(date(year,1,1).toordinal() + day - 1)

    
def return_week_number(full_date):
    '''
    Returns the week number
    Input: datetime.date full_date
    Output: int
    '''
    return full_date.isocalendar()[1]


def split_date(date_string):
    '''
    Returns year and day of the year
    Input: str date string
    Output: int year, int day_of_year
    9 characters, 4 for year, 2 additional and 
    the last three chars represent the day
    '''
    year = int(date_string[0:4])
    day_of_year = int(date_string[6:])
    
    return day_of_year, year
    
def get_week_number(date_string):
    '''
    Returns the week number given a date string
    Input: int date_string
    Output: int week_number
    '''
    day_of_year, year = split_date(str(date_string))
    full_date = date.fromordinal(date(year,1,1).toordinal() + day_of_year - 1)
    #print full_date.month
    #print full_date.year
    week_number = full_date.isocalendar()[1]
    return week_number

def get_year(date_string):
    '''
    Returns the year given a date_string
    Input: int date_string
    Outpur: int year
    '''
    _, year = split_date(str(date_string))
    return year
    
def get_month(date_string):
    '''
    Returns the month given a date_string
    Input: int date_string
    Output: int month
    '''
    day_of_year, year = split_date(str(date_string))
    full_date = date.fromordinal(date(year,1,1).toordinal() + day_of_year - 1)
    month = full_date.month
    return month
    
def rank_column_value(values):
    pass
    
####################################################
####################################################
if __name__ == "__main__":
    #date_string = "201800151"
    ##day_of_year, year = split_date(date_string)
    #print day_of_year
    #print year
    ##full_date = return_full_date(day_of_year, year)
    #print full_date
    ##week_number = return_week_number(full_date)
    #week_number,full_date = get_week_number(date_string)
    #print 'Week number: %d, for entry %s' % (week_number, date_string)
    
    ######################
    df = pd.DataFrame({'date_string':[201800151, 201800167, 201800183],
                       'value':[1.2, 3.2, 5.6]})

    df['year'] = df['date_string'].apply(get_year)
    df['month'] = df['date_string'].apply(get_month)
    df['week_num'] = df['date_string'].apply(get_week_number)
    df['ranked_score'] = df['value'].rank(ascending=False)
    print df
    

    