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
    Call the other auxiliary functions
    '''
    day_of_year, year = split_date(date_string)
    full_date = return_full_date(day_of_year, year)
    week_number = return_week_number(full_date)
    return week_number,full_date
    
####################################################
####################################################
if __name__ == "__main__":
    date_string = "201800155"
    
    ##day_of_year, year = split_date(date_string)
    #print day_of_year
    #print year
    ##full_date = return_full_date(day_of_year, year)
    #print full_date
    ##week_number = return_week_number(full_date)
    
    week_number,full_date = get_week_number(date_string)
    print 'Week number: %d, for entry %s (%s)' % (week_number, date_string, full_date)