#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  5 14:42:42 2018

@author: eduardo
"""
#https://www.blog.pythonlibrary.org/2016/08/02/python-201-a-multiprocessing-tutorial/

from multiprocessing import Process, Queue, Manager
 
sentinel = -1
 
def creator(data, q):
    """
    Creates data to be consumed and waits for the consumer
    to finish processing
    """
    print('Creating data and putting it on the queue')
    for item in data:
 
        q.put(item)
 
 
def my_consumer(q,result):
    """
    Consumes some data and works on it
 
    In this case, all it does is double the input
    """
    while True:
        data = q.get()
        print('data found to be processed: {}'.format(data))
        #processed = data * 2
        year = data[0:4]
        day_of_year = data[4:]
        #print(processed)
        print year,day_of_year
        result.append((int(year),int(day_of_year)))
        #if data is sentinel:
        size = q.qsize()
        if size == 0:
            break
        
    
 
if __name__ == '__main__':
    q = Queue()
    #data = [5, 10, 13, -1, 3]
    data = ['200040','200050','200060','200070']
    
    manager = Manager()  # create SyncManager
    result = manager.list()  # create a shared list

    process_one = Process(target=creator, args=(data, q))
    process_two = Process(target=my_consumer, args=(q,result,))
    process_one.start()
    process_two.start()
 
    q.close()
    q.join_thread()
 
    process_one.join()
    process_two.join()
    
    print(result)
    