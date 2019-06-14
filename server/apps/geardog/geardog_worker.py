#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  gearcenter_worker.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo gearcenter 工作线程

import sys
import subprocess
import os
import time
import datetime
import time
import threading
import json
import pymongo
import string
import logging
import logging.config
import uuid
import redis
import hashlib
import urllib
import base64
import random
from bson.objectid import ObjectId
if '/opt/Keeprapid/Areca/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Areca/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')


class QueryLocgic(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(QueryLocgic, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("QueryLocgic :running in __init__")

        fileobj = open("/opt/Keeprapid/Areca/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:Geardog"
        if 'geardog' in _config:
            if 'Consumer_Queue_Name' in _config['query']:
                self.recv_queue_name = _config['query']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.member
        self.collect_memberinfo = self.db.memberinfo
#        self.deviceconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.deviceconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.deviceconn.device
        self.collect_gearinfo = self.dbgear.device_info
        self.collect_vdeviceinfo = self.dbgear.vdevice_info

    def run(self):
        logger.debug("Start Geardog pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        try:
#        if 1:
            while 1:
                logger.error("device dog runinng.... ")
                time1 = time.time()
#                devicelist = set()
                lastonlinedevicelist = set()
                nowonlinedevicelist = set()
                dbofflinedevicelist = set()

#                updateonlinedevicelist = set()
                updateofflinedevicelist = set()
                updateonlinedeicelist = set()

#                v_devicelist = set()
                lastonline_v_devicelist = set()
                nowonline_v_devicelist = set()

#                updateonline_v_devicelist = set()
                updateoffline_v_devicelist = set()

#                searchkey = self.KEY_IMEI_ID % ('*','*')
#                resultlist = self._redis.keys(searchkey)
#                for devicekey in resultlist:
#                    deviceid = devicekey.split(':')[2]
#                    devicelist.add(deviceid)

#                searchkey = self.KEY_APPID_VDID % ('*','*')
#                resultlist = self._redis.keys(searchkey)
#                for vdevicekey in resultlist:
#                    vdeviceid = vdevicekey.split(':')[3]
#                    appid = vdevicekey.split(':')[2]
#                    v_devicelist.add("%s:%s" % (appid, vdeviceid))

                if self._redis.exists(self.KEY_ONLINE_DEVICE_LIST):
                    lastonlinedevicelist = eval(self._redis.get(self.KEY_ONLINE_DEVICE_LIST))

                if self._redis.exists(self.KEY_ONLINE_APPID_VDID_LIST):
                    lastonline_v_devicelist = eval(self._redis.get(self.KEY_ONLINE_APPID_VDID_LIST))

                searchkey = self.KEY_DEVICE_ONLINE_FLAG % ('*')
                resultlist = self._redis.keys(searchkey)
                for onlinekey in resultlist:
                    deviceid = onlinekey.split(':')[2]
                    nowonlinedevicelist.add(deviceid)

                #数据库中留存的脏数据
                dbonlinedeviceinfo_iter = self.collect_gearinfo.find({'state':{'$in':[self.GEAR_STATE_ACTIVE,self.GEAR_STATE_ALARM]}})
                for deviceinfo in dbonlinedeviceinfo_iter:
                    lastonlinedevicelist.add(deviceinfo['deviceid'])

                dbonline_v_deviceinfo_iter = self.collect_vdeviceinfo.find({'state':{'$in':[self.GEAR_STATE_ACTIVE,self.GEAR_STATE_ALARM]}})
                for vdeviceinfo in dbonline_v_deviceinfo_iter:
                    lastonline_v_devicelist.add("%s:%s" % (vdeviceinfo['appid'], vdeviceinfo['vdeviceid']))

                #查找db中离线的设备
                dbofflinedeviceinfo_iter = self.collect_gearinfo.find({'state':self.GEAR_STATE_OOS})
                for offline_deviceinfo in dbofflinedeviceinfo_iter:
                    dbofflinedevicelist.add(offline_deviceinfo['deviceid'])


                searchkey = self.KEY_ONLINE_APPID_VDID % ('*','*')
                resultlist = self._redis.keys(searchkey)
                for onlinekey in resultlist:
                    vdeviceid = onlinekey.split(':')[3]
                    appid = onlinekey.split(':')[2]
                    nowonline_v_devicelist.add("%s:%s" % (appid, vdeviceid))

                #根据之前的在线设备和当前的在线设备的区别，就可以判断出哪些设备不在线了
                updateofflinedevicelist = lastonlinedevicelist.difference(nowonlinedevicelist)
                updateoffline_v_devicelist = lastonline_v_devicelist.difference(nowonline_v_devicelist)
                #根据当前在线的列表和当前数据库离线的列表来比对，能够找出当前数据库离线的但实际是在线的设备
                updateonlinedeicelist = nowonlinedevicelist.intersection(dbofflinedevicelist)
#                logger.debug(lastonlinedevicelist)
#                logger.debug(nowonlinedevicelist)
#                logger.debug(updateofflinedevicelist)
#                logger.debug(lastonline_v_devicelist)
#                logger.debug(nowonline_v_devicelist)
#                logger.debug(updateoffline_v_devicelist)
                #在redis中保存当前的在线列表
                self._redis.set(self.KEY_ONLINE_DEVICE_LIST, nowonlinedevicelist)
                self._redis.set(self.KEY_ONLINE_APPID_VDID_LIST, nowonline_v_devicelist)

                newstate = self.GEAR_STATE_OOS
                #通知本该在线的设备，同时修改数据库中的状态
                for deviceid in updateonlinedeicelist:
                    searchkey = self.KEY_IMEI_ID % (deviceid, '*')
                    resultlist = self._redis.keys(searchkey)
                    if len(resultlist):
                        deviceinfo = self._redis.hgetall(resultlist[0])
                        currentstate = int(deviceinfo.get('state'))
                        logger.error("Device [%s],state DB(OOS) --> %d"%(deviceid, currentstate))
                        self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':{'state':currentstate}})
                        
                #通知哪些不在线的设备状态，同时修改数据库中的状态
                for deviceid in updateofflinedevicelist:
                    searchkey = self.KEY_IMEI_ID % (deviceid, '*')
                    resultlist = self._redis.keys(searchkey)
                    if len(resultlist):
                        deviceinfo = self._redis.hgetall(resultlist[0])
                        self._redis.hset(resultlist[0],'state',newstate)
                        logger.error("Device [%s],state XX --> %d"%(deviceid, self.GEAR_STATE_OOS))
#                        self.collect_gearinfo.update({'deviceid':deviceid},{'$set':{'state':newstate}})
                        memberlist = eval(deviceinfo.get('member_id'))
                        if memberlist is None:
                            continue
                        if isinstance(memberlist,list):
                            for member_id in memberlist:
                                searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', member_id)
                                resultlist = self._redis.keys(searchkey)
                                if len(resultlist):
                                    username = resultlist[0].split(':')[2]
                                    senddict = dict()
                                    senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                                    tmpdict = dict()
                                    tmpdict['action_cmd'] = 'device_state'
                                    tmpdict['seq_id'] = 1
                                    tmpdict['version'] = '1.0'
                                    tmpbody = dict()
                                    tmpdict['body'] = tmpbody
                                    tmpbody['deviceid'] = deviceid
                                    tmpbody['state'] = newstate
                                    senddict['sendbuf'] = json.dumps(tmpdict)
                                    #发送消息
                                    if 'mqtt_publish' in self._config:
                                        self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

                        else:
                            searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', deviceinfo.get('member_id'))
                            resultlist = self._redis.keys(searchkey)
                            if len(resultlist):
                                username = resultlist[0].split(':')[2]
                                senddict = dict()
                                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                                tmpdict = dict()
                                tmpdict['action_cmd'] = 'device_state'
                                tmpdict['seq_id'] = 1
                                tmpdict['version'] = '1.0'
                                tmpbody = dict()
                                tmpdict['body'] = tmpbody
                                tmpbody['deviceid'] = deviceid
                                tmpbody['state'] = newstate
                                senddict['sendbuf'] = json.dumps(tmpdict)
                                #发送消息
                                if 'mqtt_publish' in self._config:
                                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
                    self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':{'state':newstate}})

                for app_vdeviceid in updateoffline_v_devicelist:
                    appid = app_vdeviceid.split(':')[0]
                    vdeviceid = app_vdeviceid.split(':')[1]
                    searchkey = self.KEY_APPID_VDID % (appid, vdeviceid)
                    resultlist = self._redis.keys(searchkey)
                    logger.error("Vdevice [%s:%s],state XX --> %d"%(appid,vdeviceid, self.GEAR_STATE_OOS))
                    if len(resultlist):
                        deviceinfo = self._redis.hgetall(resultlist[0])
                        self._redis.hset(resultlist[0],'state',newstate)
#                        self.collect_vdeviceinfo.update({'vdeviceid':vdeviceid,'appid':appid},{'$set':{'state':newstate}})
                        memberlist = eval(deviceinfo.get('member_id'))
                        if memberlist is None:
                            continue
                        if isinstance(memberlist,list):
                            for member_id in memberlist:
                                searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', member_id)
                                resultlist = self._redis.keys(searchkey)
                                if len(resultlist):
                                    username = resultlist[0].split(':')[2]
                                    senddict = dict()
                                    senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                                    tmpdict = dict()
                                    tmpdict['action_cmd'] = 'vdevice_state'
                                    tmpdict['seq_id'] = 1
                                    tmpdict['version'] = '1.0'
                                    tmpbody = dict()
                                    tmpdict['body'] = tmpbody
                                    tmpbody['appid'] = appid
                                    tmpbody['vdeviceid'] = vdeviceid
                                    tmpbody['state'] = newstate
                                    senddict['sendbuf'] = json.dumps(tmpdict)
                                    #发送消息
                                    if 'mqtt_publish' in self._config:
                                        self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

                        else:
                            searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', deviceinfo.get('member_id'))
                            resultlist = self._redis.keys(searchkey)
                            if len(resultlist):
                                username = resultlist[0].split(':')[2]
                                senddict = dict()
                                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                                tmpdict = dict()
                                tmpdict['action_cmd'] = 'vdevice_state'
                                tmpdict['seq_id'] = 1
                                tmpdict['version'] = '1.0'
                                tmpbody = dict()
                                tmpdict['body'] = tmpbody
                                tmpbody['appid'] = appid
                                tmpbody['vdeviceid'] = vdeviceid
                                tmpbody['state'] = newstate
                                senddict['sendbuf'] = json.dumps(tmpdict)
                                #发送消息
                                if 'mqtt_publish' in self._config:
                                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
                    self.collect_vdeviceinfo.update_one({'vdeviceid':vdeviceid},{'$set':{'state':newstate}})
                

                #在这里检查DI4报警的状态
                searchDI4key = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % ('*','DI',"4")
                resultlist = self._redis.keys(searchDI4key)
                for key in resultlist:
                    deviceid = key.split(':')[2]
                    ad2alarmkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid,'AD','2')
                    ad2alarmcreatedkey = self.KEY_AD2_CREATED_ALARM % (deviceid)
                    di4alarmtimeoutkey = self.KEY_DI4_ALARM_FLAG % (deviceid)
                    devicinfokeysearch = self.KEY_IMEI_ID % (deviceid,'*')
                    searchdeviceresult = self._redis.keys(devicinfokeysearch)
                    objectid = ''
                    if len(searchdeviceresult):
                        objectid = searchdeviceresult[0].split(':')[3]

                    if self._redis.exists(ad2alarmkey) is True and self._redis.exists(ad2alarmcreatedkey) is False and self._redis.exists(di4alarmtimeoutkey) is False:
                        value = float(self._redis.get(ad2alarmkey))
                        logger.error("create alarm [%s]->AD2[%r]" %(deviceid, value))
                        alarmlist = list()
                        alarminfo = dict()
                        alarminfo['deviceid'] = deviceid
                        alarminfo['objectid'] = objectid
                        alarminfo['level'] = self.ALARM_LEVEL_INTERFACE
                        alarminfo['value'] = value
                        alarminfo['vdeviceid'] = ''
                        alarminfo['appid'] = ''
                        alarminfo['interface'] = 'AD'
                        alarminfo['channel'] = 2
                        alarminfo['ifname'] = 'AD2'
                        alarminfo['current_state'] = 1
                        alarminfo['origin_state'] = 0
                        alarminfo['alarm_type'] = self.ALARM_TYPE_DEVICE_MAINTENANCE
                        alarmlist.append(alarminfo)
                        msgdict = dict()
                        msgdict['action_cmd'] = 'push_alarm_list'
                        msgdict['body'] = dict()
                        msgdict['body']['alarmlist'] = alarmlist
        #                msgdict['body']['memberlist'] = eval(imeiinfo.get('member_id'))
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'alarmcenter' in self._config:
                            logger.error("send alarm[%s] AD2 alarm"% (deviceid))
                            self._redis.lpush(self._config['alarmcenter']['Consumer_Queue_Name'],json.dumps(msgdict))
                            #设置AD2已经告过警了
                            self._redis.set(ad2alarmcreatedkey,'1')

                logger.error("=====>dog done, in %f"%(time.time()-time1))
                time.sleep(self.DOG_SLEEP_TIME)

        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))


#    def run(self):
#        logger.debug("Start Geardog pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
##        if 1:
#            while 1:
#                logger.debug("device dog runinng.... ")
#                time1 = time.time()
#                searchkey = self.KEY_IMEI_ID % ('*','*')
#                resultlist = self._redis.keys(searchkey)
#                for devicekey in resultlist:
#                    deviceinfo = self._redis.hgetall(devicekey)
#                    #检查在线标志
#                    deviceid = devicekey.split(':')[2]
#                    isonline = False
#                    onlinekey = self.KEY_DEVICE_ONLINE_FLAG % (deviceid)
#                    if self._redis.exists(onlinekey) is True:
#                        isonline = True
#                    #检查是否有最近的数据 来判断设备是否启用
#                    haslastdata = False
#                    lastkey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid)
#                    if self._redis.exists(lastkey) is True:
#                        haslastdata = True
#
#                    currentstate = deviceinfo.get('state')
#                    if currentstate is None:
#                        currentstate = self.GEAR_STATE_NEW
#                    if isinstance(currentstate, str):
#                        currentstate = int(currentstate)
#
#                    newstate = self.GEAR_STATE_ACTIVE
#                    need_sendalarm = False
#                    if isonline == False:
#                        if currentstate not in [self.GEAR_STATE_OOS, self.GEAR_STATE_NEW]:
#                            newstate = self.GEAR_STATE_OOS
#                            need_sendalarm = True
#                            self._redis.hset(devicekey,'state',newstate)
#                            self.collect_gearinfo.update({'deviceid':deviceid},{'$set':{'state':newstate}})
#                    else:
#                        if currentstate == self.GEAR_STATE_OOS:
#                            newstate = self.GEAR_STATE_ACTIVE
#                            need_sendalarm = True
#                            self._redis.hset(devicekey,'state',newstate)
#                            self.collect_gearinfo.update({'deviceid':deviceid},{'$set':{'state':newstate}})
#
#                    if need_sendalarm:
#                        logger.debug("====>need send dog alarm,[%s],state[%d -> %d]"%(deviceid, currentstate, newstate))
#                        memberlist = eval(deviceinfo.get('member_id'))
#                        if memberlist is None:
#                            continue
#
#                        if isinstance(memberlist,list):
#                            for member_id in memberlist:
#                                searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', member_id)
#                                resultlist = self._redis.keys(searchkey)
#                                if len(resultlist):
#                                    username = resultlist[0].split(':')[2]
#                                    senddict = dict()
#                                    senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
#                                    tmpdict = dict()
#                                    tmpdict['action_cmd'] = 'device_state'
#                                    tmpdict['seq_id'] = 1
#                                    tmpdict['version'] = '1.0'
#                                    tmpbody = dict()
#                                    tmpdict['body'] = tmpbody
#                                    tmpbody['deviceid'] = deviceid
#                                    tmpbody['state'] = newstate
#                                    senddict['sendbuf'] = json.dumps(tmpdict)
#                                    #发送消息
#                                    if 'mqtt_publish' in self._config:
#                                        self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
#
#                        else:
#                            searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', deviceinfo.get('member_id'))
#                            resultlist = self._redis.keys(searchkey)
#                            if len(resultlist):
#                                username = resultlist[0].split(':')[2]
#                                senddict = dict()
#                                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
#                                tmpdict = dict()
#                                tmpdict['action_cmd'] = 'device_state'
#                                tmpdict['seq_id'] = 1
#                                tmpdict['version'] = '1.0'
#                                tmpbody = dict()
#                                tmpdict['body'] = tmpbody
#                                tmpbody['deviceid'] = deviceid
#                                tmpbody['state'] = newstate
#                                senddict['sendbuf'] = json.dumps(tmpdict)
#                                #发送消息
#                                if 'mqtt_publish' in self._config:
#                                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
#
#                logger.debug("=====>dog done, in %f"%(time.time()-time1))
#                time.sleep(self.DOG_SLEEP_TIME)
#
#        except Exception as e:
#            logger.debug("%s except raised : %s " % (e.__class__, e.args))
#




if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'member' in _config and _config['member'] is not None:
        if 'thread_count' in _config['member'] and _config['member']['thread_count'] is not None:
            thread_count = int(_config['member']['thread_count'])

    for i in xrange(0, thread_count):
        obj = QueryLocgic(i)
        obj.setDaemon(True)
        obj.start()

    while 1:
        time.sleep(1)
