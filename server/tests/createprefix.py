#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
#给数据库中的device增加prefix，修复member_id, 和增加ownerid
import socket
import time
import threading
import sys
import redis
import json
import pymongo
from bson.objectid import ObjectId

conn = pymongo.Connection('mongodb://admin:Kr123$^@localhost:27017/')
db = conn.device
col = db.device_info
col_log = db.device_log

mconn = pymongo.Connection('mongodb://admin:Kr123$^@localhost:27017/')
mdb = mconn.member
mcol = mdb.memberinfo

r = redis.StrictRedis(password='Kr123456')

dbgears = col.find({})
i = 0
for dbgearinfo in dbgears:
    imei = dbgearinfo['deviceid']
    imeistr = str(imei)
    prefix = imeistr[0:3]
    col.update_one({'_id':ObjectId(dbgearinfo['_id'].__str__())},{'$set':{'prefix':prefix}})

    print imeistr,prefix

    memberlist = dbgearinfo.get('member_id')
    key = "A:di:%s:%s" % (imeistr, dbgearinfo['_id'].__str__())
    print memberlist
    setdict = dict()
    if memberlist is None or len(memberlist)==0:
        memberlist = list()
        loginfo = col_log.find_one({'action':'add','device_id':dbgearinfo['_id'].__str__()})
        if loginfo is None:  
            setdict['member_id'] = memberlist
            setdict['ownerid'] = ''
            print 'case 1.1'
        else:
            memberlist.append(loginfo['member_id'])
            setdict['member_id'] = memberlist
            setdict['ownerid'] = loginfo['member_id']
            print 'case 1.2'
    else:
        if len(memberlist)>=10:
            memberid = ''.join(memberlist)
            memberlist2 = list()
            memberlist2.append(memberid)
            setdict['member_id'] = memberlist2
            setdict['ownerid'] = memberid
            print 'case 2'

        else:
            memberid = memberlist[0]
            setdict['ownerid'] = memberid
            print 'case 3'

    print setdict
    col.update_one({'_id':ObjectId(dbgearinfo['_id'].__str__())},{'$set':setdict})
    r.hmset(key,setdict)



