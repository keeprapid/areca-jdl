#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  transaction_worker.py
# creator:   jacob.qian
# datetime:  2016-6-20
# 处理业务逻辑

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


class Transaction(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(Transaction, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("Transaction :running in __init__")

        fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:Transaction"
        if 'transaction' in _config:
            if 'Consumer_Queue_Name' in _config['transaction']:
                self.recv_queue_name = _config['transaction']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.member
        self.collect_memberinfo = self.db.memberinfo
#        self.collect_memberlog = self.db.memberlog
#        self.deviceconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.deviceconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.deviceconn.device
        self.collect_gearinfo = self.dbgear.device_info
#        self.collect_gearlog = self.dbgear.device_log
#        self.collect_template = self.dbgear.device_template
        self.collect_appinfo = self.dbgear.app_info
        self.collect_vdevice = self.dbgear.vdevice_info

    def run(self):
        logger.debug("Start Transaction pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                # logger.debug("_proc_message cost %f" % (time.time()-t1))
                    
#        except Exception as e:
#            logger.debug("%s except raised : %s " % (e.__class__, e.args))

    #递归调用，解析每一层可能需要urllib.unquote的地方
    def parse_param_lowlevel(self, obj):
        if isinstance(obj,dict):
            a =dict()
            for key in obj:
                if isinstance(obj[key],dict) or isinstance(obj[key],list):
                    a[key] = self.parse_param_lowlevel(obj[key])
                elif isinstance(obj[key],unicode) or isinstance(obj[key],str):
                    a[key] = urllib.unquote(obj[key].encode('utf-8')).decode('utf-8')
                else:
                    a[key] = obj[key]
            return a
        elif isinstance(obj,list):
            b = list()
            for l in obj:
                if isinstance(l, dict) or isinstance(l, list):
                    b.append(self.parse_param_lowlevel(l))
                elif isinstance(l,unicode) or isinstance(l,str):
                    b.append(urllib.unquote(l.encode('utf-8')).decode('utf-8'))
                else:
                    b.append(l)
            return b
        else:
            return obj

    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
#        logger.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
#            logger.debug(recvbuf)
            msgdict = json.loads(recvbuf)
        except:
            logger.error("parse body error")
            return
        #检查消息必选项
        if len(msgdict) == 0:
            logger.error("body lenght is zero")
            return
        if "from" not in msgdict:
            logger.error("no route in body")
            return
        msgfrom = msgdict['from']

        seqid = '0'
        if "seqid" in msgdict:
            seqid = msgdict['seqid']

        sockid = ''
        if 'sockid' in msgdict:
            sockid = msgdict['sockid']

        if "action_cmd" not in msgdict:
            logger.error("no action_cmd in msg")
            self._sendMessage(msgfrom, '{"from":%s,"error_code":"40000","seq_id":%s,"body":{},"sockid":%s)' % (self.recv_queue_name, seqid, sockid))
            return
        #构建回应消息结构
        action_cmd = msgdict['action_cmd']

        message_resp_dict = dict()
        message_resp_dict['from'] = self.recv_queue_name
        message_resp_dict['seq_id'] = seqid
        message_resp_dict['sockid'] = sockid
        message_resp_body = dict()
        message_resp_dict['body'] = message_resp_body
        
        self._proc_action(msgdict, message_resp_dict, message_resp_body)

        msg_resp = json.dumps(message_resp_dict)
#        logger.debug(msg_resp)
        self._sendMessage(msgfrom, msg_resp)   

    def _proc_action(self, msg_in, msg_out_head, msg_out_body):
        '''action处理入口函数'''
#        logger.debug("_proc_action action=%s" % (action))
        if 'action_cmd' not in msg_in or 'version' not in msg_in:
            logger.error("mandotry param error in action")
            msg_out_head['error_code'] = '40002'
            return
        action_cmd = msg_in['action_cmd']
#        logger.debug('action_cmd : %s' % (action_cmd))
        action_version = msg_in['version']
#        logger.debug('action_version : %s' % (action_version))
        if 'body' in msg_in:
            action_body = msg_in['body']
#            logger.debug('action_body : %s' % (action_body))
        else:
            action_body = None
            logger.debug('no action_body')

        if action_cmd == 'calc_device_state':
            self._proc_action_calc_device_state(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return


    def _proc_action_calc_device_state(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'tid'    : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_calc_device_state action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('deviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['deviceid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            recvtimestamp = datetime.datetime.now()

            deviceid = action_body['deviceid']

            searchkey = self.KEY_IMEI_ID % (deviceid, '*')
            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return

            imeikey = resultlist[0]
            deviceinfo = self._redis.hgetall(imeikey)

            #设置online标志
            onlinekey = self.KEY_DEVICE_ONLINE_FLAG % (deviceid)
            timeout = self.GEAR_ONLINE_TIMEOUT
            if 'd_timeout' in deviceinfo:
                timeout = int(deviceinfo['d_timeout'])
            #如果是已经在线的状态话，就要判断当前设备状态是否有变迁
            if self._redis.exists(onlinekey):
                self._redis.expire(onlinekey,timeout)
                alarmkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid, '*', '*')
                resultlist = self._redis.keys(alarmkey)
                newstate = self.GEAR_STATE_ACTIVE
                if len(resultlist):
                    newstate = self.GEAR_STATE_ALARM
                laststate = deviceinfo.get('state')
                if laststate == None or int(laststate) != newstate:
                    logger.error("Device [%s],state %s --> %s"%(deviceid, laststate, newstate))
                    #前后状态不一致时，开始通知
                    setdict = dict()
                    setdict['state'] = newstate
                    self._redis.hmset(imeikey, setdict)
                    self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':setdict})
                    self.sendDeviceStateMqttNotify(deviceid, newstate, deviceinfo)

            else:
                #如果之前没有在线标记，那么在这里设置在线状态（正常，或者告警)
                self._redis.set(onlinekey, recvtimestamp.__str__())
                self._redis.expire(onlinekey,timeout)
                alarmkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid, '*', '*')
                resultlist = self._redis.keys(alarmkey)
                newstate = self.GEAR_STATE_ACTIVE
                if len(resultlist):
                    newstate = self.GEAR_STATE_ALARM
                setdict = dict()
                setdict['state'] = newstate
                logger.error("Device [%s],state %s --> %s"%(deviceid, self.GEAR_STATE_OOS, newstate))
                self._redis.hmset(imeikey, setdict)
                self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':setdict})
                self.sendDeviceStateMqttNotify(deviceid, newstate, deviceinfo)


            #设置app vdid的online标志,先根据Deviceid找到归属的所有的VDevice，然后根据VDevice下属的所有Device 的alarm字段判断这个Vdevice的状态。
            searchkey = self.KEY_APPID_VDID_DID % ('*','*',deviceid)
            resultlist = self._redis.keys(searchkey)
            for avdkey in resultlist:
                klist = avdkey.split(':')
                appid = klist[2]
                vdeviceid = klist[3]
                vdeviceinfokey = self.KEY_APPID_VDID %(appid, vdeviceid)
                vdeviceinfo = self._redis.hgetall(vdeviceinfokey)
                #如果之前没有在线标记，那么在这里设置在线状态（正常，或者告警)
                setkey = self.KEY_ONLINE_APPID_VDID % (appid, vdeviceid)
                timeout = self.GEAR_ONLINE_TIMEOUT
                if 'd_timeout' in vdeviceinfo:
                    timeout = int(vdeviceinfo['d_timeout'])

                if self._redis.exists(setkey):
                    self._redis.expire(setkey,self.GEAR_ONLINE_TIMEOUT)
                    laststate = vdeviceinfo.get('state')
                    newstate = self.GEAR_STATE_ACTIVE
                    searchkey = self.KEY_APPID_VDID_DID % ('*',vdeviceid,'*')
                    resultlist = self._redis.keys(searchkey)
                    for vdevicekey in resultlist:
                        searchdeviceid = vdevicekey.split(':')[4]
                        alarmsearchkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (searchdeviceid, '*', '*')
                        alarmresultlist = self._redis.keys(alarmsearchkey)
                        if len(alarmresultlist):
                            newstate = self.GEAR_STATE_ALARM
                            break

                    if laststate is None or int(laststate) != newstate:
                        logger.debug("Vdevice [%s:%s],state %s --> %s"%(appid,vdeviceid, int(laststate), newstate))
                        self._redis.hset(vdeviceinfokey, 'state', newstate)
                        self.collect_vdevice.update_one({'appid':appid,'vdeviceid':vdeviceid},{'$set':{'state':newstate}})
                        self.send_V_DeviceStateMqttNotify(appid, vdeviceid, newstate, vdeviceinfo)
                else:
                    self._redis.set(setkey, recvtimestamp.__str__())
                    self._redis.expire(setkey,self.GEAR_ONLINE_TIMEOUT)
#                    alarmkey = self.KEY_ALARM_APPID_VDID % (appid, vdeviceid)
                    newstate = self.GEAR_STATE_ACTIVE
                    searchkey = self.KEY_APPID_VDID_DID % ('*',vdeviceid,'*')
                    resultlist = self._redis.keys(searchkey)
                    for vdevicekey in resultlist:
                        searchdeviceid = vdevicekey.split(':')[4]
                        alarmsearchkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (searchdeviceid, '*', '*')
                        alarmresultlist = self._redis.keys(alarmsearchkey)
                        if len(alarmresultlist):
                            newstate = self.GEAR_STATE_ALARM
                            break

#                    if self._redis.exists(alarmkey):
#                        newstate = self.GEAR_STATE_ALARM
                    logger.debug("Vdevice [%s:%s],state %s --> %s"%(appid,vdeviceid, self.GEAR_STATE_OOS, newstate))
                    self._redis.hset(vdeviceinfokey, 'state', newstate)
                    self.collect_vdevice.update_one({'appid':appid,'vdeviceid':vdeviceid},{'$set':{'state':newstate}})
                    self.send_V_DeviceStateMqttNotify(appid, vdeviceid, newstate, vdeviceinfo)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def sendDeviceStateMqttNotify(self, deviceid, state, deviceinfo):
        logger.debug(" into sendDeviceStateMqttNotify %s:%d"% (deviceid, state))    
        newstate = state    
        memberlist = eval(deviceinfo.get('member_id'))
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

    def send_V_DeviceStateMqttNotify(self, appid, vdeviceid, state, vdeviceinfo):
        logger.debug(" into send_V_DeviceStateMqttNotify [%s:%s]:%d"% (appid, vdeviceid, state))    
        newstate = state    
        memberlist = eval(vdeviceinfo.get('member_id'))
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
            searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', vdeviceinfo.get('member_id'))
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
                tmpbody['appid'] = appid
                tmpbody['vdeviceid'] = vdeviceid
                tmpbody['state'] = newstate
                senddict['sendbuf'] = json.dumps(tmpdict)
                #发送消息
                if 'mqtt_publish' in self._config:
                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'transaction' in _config and _config['transaction'] is not None:
        if 'thread_count' in _config['transaction'] and _config['transaction']['thread_count'] is not None:
            thread_count = int(_config['transaction']['thread_count'])

    for i in xrange(0, thread_count):
        obj = Transaction(i)
        obj.setDaemon(True)
        obj.start()

    while 1:
        time.sleep(1)
