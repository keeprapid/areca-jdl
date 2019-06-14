#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

import time
import sys
import pymongo

def delrecord(number):
    t1 = time.time()
    conn = pymongo.MongoClient('mongodb://admin:Kr123$^@localhost:27017')
    db=conn.datacenter
    col=db.data_info
    i = 0
    for row in col.find().sort('_id').limit(number):
        i+=1
        if i%1000==0:
            print "%.2f%%" % ((i/float(number))*100)
#            print row
        #print row
        col.remove({'_id':row['_id']})
    print time.time()-t1

delrecord(100000000)




