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

import logging
import logging.config
import uuid
import redis
import hashlib
import urllib
import base64
import random
from bson.objectid import ObjectId
from bson import json_util

if '/opt/Keeprapid/Areca/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Areca/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')


class DataNotify(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(DataNotify, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("DataNotify :running in __init__")

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
        self.recv_queue_name = "W:Queue:Member"
        if 'datanotify' in _config:
            if 'Consumer_Queue_Name' in _config['datanotify']:
                self.recv_queue_name = _config['datanotify']['Consumer_Queue_Name']

        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
#        self.conn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.conn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.datadb = self.conn.datacenter
        self.collect_data = self.datadb.data_info

#        self.deviceconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.deviceconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.deviceconn.device
        self.collect_gearinfo = self.dbgear.device_info

#        self.ctrlconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.ctrlconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbctrl = self.ctrlconn.control
        self.collect_ctrl = self.dbctrl.control_log

    def run(self):
        logger.debug("Start DataNotify pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        try:
#        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))                    
                    
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))


    def getFloatfromBuf(self, buf):
        index = buf.find('.')
        if index == -1:
            return 0,0

        endindex = index+3
        return endindex,float(buf[:endindex])


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

        if action_cmd == 'data_notify':
            self._proc_action_data_notify(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_data_notify(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     
                        vid     
                        imei_id M   
                        start_time  O   
                        end_time    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_data_notify action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('notifymsglist' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['notifymsglist'] is None or action_body['notifymsglist'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            notifymsglist = action_body['notifymsglist']

            need_devicealarm = False
            alarmtype = self.GEAR_ALARM_INVALID
            if len(notifymsglist):
                #存最近的值
                ifname = insertdata.get('if_name')
                if cwdtype not in ['GM67','RS45','CK83','HT99','LB66']:
                    lastkey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid)
                    resultlist = self._redis.keys(lastkey)
                    if len(resultlist) == 0:
                        lastinfodict = dict()
                    else:
                        lastinfodict = self._redis.hgetall(lastkey)
                        if lastinfodict is None:
                            lastinfodict = dict()
#                        else:
#                            lastinfodict = eval(lastinfodict)
                    
                    for datainfo in notifymsglist:
                        ifkey = datainfo['if_name']
                        if 'channel' in datainfo and datainfo['channel'] != '':
                            ifkey = "%s%d" % (ifkey, datainfo['channel'])
                        ifinfodict = dict()
                        for key in datainfo:
                            if key in ['timestamp','recv_time','_id','recordtime']:
                                ifinfodict[key] = datainfo[key].__str__()
                            else:
                                ifinfodict[key] = datainfo[key]
                        lastinfodict[ifkey] = ifinfodict
                    lastinfodict['last_time'] = recvtimestamp.__str__()
#                    logger.debug(lastinfodict)
#                   判断是否要产生设备报警通知
                    newstate = self.GEAR_STATE_ACTIVE
                    currentstate = imeiinfo['state']
                    if isinstance(currentstate,str):
                        currentstate = int(currentstate)

                    for key in lastinfodict:
                        if key in ['last_time']:
                            continue
                        ifinfo = lastinfodict[key]
                        if isinstance(ifinfo, str):
                            ifinfo = eval(lastinfodict[key])
                        if 'state' in ifinfo:
                            ifname = ifinfo.get('if_name')
                            if ifname in ['DO','AS','arm_state']:
                                continue
                            state = ifinfo['state']

                            if isinstance(state,int) is False:
                                state = int(state)
#                            logger.debug(ifinfo)
#                            logger.debug("state = %d",state)
                            if state == 1:
                                newstate = self.GEAR_STATE_ALARM
                                break

                    if newstate == self.GEAR_STATE_ALARM:
                        if currentstate != self.GEAR_STATE_ALARM:
                            need_devicealarm = True
                            alarmtype = self.GEAR_ALARM_CREATE
                    else:
                        if currentstate != self.GEAR_STATE_ACTIVE:
                            need_devicealarm = True
                            alarmtype = self.GEAR_ALARM_RESTORE

                    logger.debug("%s,newstate = %d, oldstate = %d, need_devicealarm=%d"%(deviceid, newstate, currentstate, need_devicealarm))
                    #更新设备状态
                    if need_devicealarm:
                        self._redis.hset(imeikey,'state',newstate)
                        self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':{'state':newstate}})

                    self._redis.hmset(lastkey,lastinfodict)





                #检查用户是否在线
                memberlist = eval(imeiinfo.get('member_id'))
#                logger.debug(memberlist)
#                logger.debug(memberlist.__class__)
                if isinstance(memberlist,list):
                    for member_id in memberlist:
                        searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', member_id)
                        resultlist = self._redis.keys(searchkey)
#                        logger.debug(resultlist)
                        if len(resultlist):
                            username = resultlist[0].split(':')[2]
                            senddict = dict()
                            senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                            tmpdict = dict()
                            tmpdict['action_cmd'] = 'data_update'
                            tmpdict['seq_id'] = 1
                            tmpdict['version'] = '1.0'
                            tmpbody = dict()
                            tmpdict['body'] = tmpbody
                            tmpbody['deviceid'] = deviceid
#                            logger.debug(notifymsglist)
                            for obj in notifymsglist:
                                if 'timestamp' in obj:
                                    obj['timestamp'] = obj['timestamp'].__str__()
                                if 'recv_time' in obj:
                                    obj['recv_time'] = obj['recv_time'].__str__()
                                if '_id' in obj:
                                    obj.pop('_id')

                            tmpbody['datalist'] = notifymsglist
                            senddict['sendbuf'] = json.dumps(tmpdict)
                            #发送消息
                            if 'mqtt_publish' in self._config:
                                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

                            if need_devicealarm:
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
                    searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', imeiinfo.get('member_id'))
                    resultlist = self._redis.keys(searchkey)
                    logger.debug(resultlist)
                    if len(resultlist):
                        username = resultlist[0].split(':')[2]
                        senddict = dict()
                        senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                        tmpdict = dict()
                        tmpdict['action_cmd'] = 'data_update'
                        tmpdict['seq_id'] = 1
                        tmpdict['version'] = '1.0'
                        tmpbody = dict()
                        tmpdict['body'] = tmpbody
                        tmpbody['deviceid'] = deviceid
                        logger.debug(notifymsglist)
                        for obj in notifymsglist:
                            if 'timestamp' in obj:
                                obj['timestamp'] = obj['timestamp'].__str__()
                            if 'recv_time' in obj:
                                obj['recv_time'] = obj['recv_time'].__str__()
                            if '_id' in obj:
                                obj.pop('_id')

                        tmpbody['datalist'] = notifymsglist
                        senddict['sendbuf'] = json.dumps(tmpdict)
                        #发送消息
                        if 'mqtt_publish' in self._config:
                            self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

                        if need_devicealarm:
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

            #查看是否有消息队列
            currentcmdkey = self.KEY_DEVICE_CURRENT_CMD % (deviceid)
            if self._redis.exists(currentcmdkey) is False:
                cachedcmdlistkey = self.KEY_DEVICE_CTRL_CMD_LIST % (deviceid)
                nextcmd = self._redis.rpop(cachedcmdlistkey)
                if nextcmd is not None:
                    sockkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid,'*')
                    resultlist = self._redis.keys(sockkey)
                    if len(resultlist):
                        sockinfo = self._redis.get(resultlist[0])
                        msgdict = dict()
                        msgdict['action_cmd'] = 'device_ctrl'
                        msgdict['body'] = dict()
                        msgdict['body']['sendbuf'] = nextcmd
                        msgdict['body']['sockid'] = sockinfo
                        msgdict['body']['deviceid'] = deviceid
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'deviceproxy' in self._config:
                            self._redis.lpush(self._config['deviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                    self._redis.set(currentcmdkey, nextcmd)
                    self._redis.expire(currentcmdkey,self.CTRL_CMD_TIMEOUT)
                    #self._redis.rpush(cachedcmdlistkey,nextcmd)

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'datanotify' in _config and _config['datanotify'] is not None:
        if 'thread_count' in _config['datanotify'] and _config['datanotify']['thread_count'] is not None:
            thread_count = int(_config['datanotify']['thread_count'])

    for i in xrange(0, thread_count):
        dataNotify = DataNotify(i)
        dataNotify.setDaemon(True)
        dataNotify.start()

    while 1:
        time.sleep(1)
