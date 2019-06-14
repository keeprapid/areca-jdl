#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

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

mconn = pymongo.Connection('mongodb://admin:Kr123$^@localhost:27017/')
mdb = mconn.member
mcol = mdb.memberinfo


r = redis.StrictRedis(password='Kr123456')

KEY_TOKEN_NAME_ID = "A:tni:%s:%s:%s"
KEY_MEMBER_IMEI = "A:md:%s:%s"
KEY_IMEI_ID = "A:di:%s:%s"
KEY_CLIENT_ONLINE_FLAG = "A:oui:%s:%s"
KEY_TEMPLATE_FLAG = "A:tp:%s"
KEY_DEVICE_INTERFACE_FLAG = "A:alarm:%s:%s"
KEY_DEVICE_LASTDATA_FLAG = "A:dl:%s"
KEY_DEVICE_ONLINE_FLAG = "A:doi:%s"

devicelist = r.keys('A:di:*')
for key in devicelist:
    print "delete key[%s]" % (key)
    r.delete(key)


dbgears = col.find({})
i = 0
for dbgearinfo in dbgears:
    imei = dbgearinfo['deviceid']
    imeistr = str(imei)
    addkey = KEY_IMEI_ID % (imeistr,dbgearinfo['_id'].__str__())
    for key in dbgearinfo:
        if key in ['_id','createtime','last_location_time']:
            dbgearinfo[key] = dbgearinfo[key].__str__()

    r.hmset(addkey, dbgearinfo)
    print addkey,dbgearinfo



