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

        fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:Query"
        if 'query' in _config:
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


    def run(self):
        logger.debug("Start QueryLocgic pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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



    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
        # logger.debug('_proc_message')
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

        if action_cmd == 'query_device_currentdata':
            self._proc_action_query_device_currentdata(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_device_currentdata2':
            self._proc_action_query_device_currentdata2(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'query_device_channels':
            self._proc_action_query_device_channels(action_version, action_body, msg_out_head, msg_out_body)

        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_query_device_currentdata(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_query_device_currentdata action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('tid' not in action_body) or  ('deviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['deviceid'] is None or action_body['deviceid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            tid = action_body['tid']
            deviceid = action_body['deviceid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
#            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            if tni.get('username') != 'admin':
                if 'device' not in tni or tni['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                    retbody['datadict'] = dict()
                    return

                devicelist = eval(tni['device'])
                if deviceid not in devicelist:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                    retbody['datadict'] = dict()
                    return
                    
            lastdatakey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid) 
            lastdatadict = self._redis.hgetall(lastdatakey)
            if lastdatadict is None:
                retdict['error_code'] = self.ERRORCODE_OK
                retbody['datadict'] = dict()
                return

            datadict = dict()
            retbody['datadict'] = datadict

            searchkey = self.KEY_IMEI_ID % (deviceid, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                deviceinfo = self._redis.hgetall(resultlist[0])
                state = deviceinfo['state']
                if isinstance(state, str):
                    state = int(state)
                retbody['state'] = state
            else:
                retbody['state'] = self.GEAR_STATE_UNKNOWN
                
            for key in lastdatadict:
                if key == 'last_time':
                    retbody[key] = lastdatadict[key]
                else:
                    datadict[key] = eval(lastdatadict[key])
                    if 'recordtime' in datadict[key]:
                        datadict[key]['recordtime'] = datadict[key]['recordtime'].__str__()

            retdict['error_code'] = self.ERRORCODE_OK
            return



        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_query_device_currentdata2(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'query_device_currentdata2', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'deviceid'    : M
                     }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        # logger.debug(" into _proc_action_query_device_currentdata2 action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('deviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['deviceid'] is None or action_body['deviceid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            deviceid = action_body['deviceid']
                    
            lastdatakey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid) 
            lastdatadict = self._redis.hgetall(lastdatakey)
            if lastdatadict is None:
                retdict['error_code'] = self.ERRORCODE_OK
                retbody['datadict'] = dict()
                return

            datadict = dict()
            retbody['datadict'] = datadict

            searchkey = self.KEY_IMEI_ID % (deviceid, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                deviceinfo = self._redis.hgetall(resultlist[0])
                state = deviceinfo['state']
                if isinstance(state, str):
                    state = int(state)
                retbody['state'] = state
            else:
                retbody['state'] = self.GEAR_STATE_UNKNOWN
                
            for key in lastdatadict:
                if key == 'last_time':
                    retbody[key] = lastdatadict[key]
                else:
                    datadict[key] = eval(lastdatadict[key])
                    if 'recordtime' in datadict[key]:
                        datadict[key]['recordtime'] = datadict[key]['recordtime'].__str__()

            retdict['error_code'] = self.ERRORCODE_OK
            return



        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_query_device_channels(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'register', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logger.debug(" into _proc_action_query_device_channels action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('tid' not in action_body) or  ('devices' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['devices'] is None or action_body['devices'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            tid = action_body['tid']
            devices = action_body['devices']
            querydevicelist = devices.keys()
            returndevicelist = list()
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
#            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            if tni.get('username') != 'admin':
                if 'device' not in tni or tni['device'] is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                    retbody['datadict'] = dict()
                    return

                devicelist = eval(tni['device'])
                devicelist_set = set(devicelist)
                devices_set = set(querydevicelist)
                returndevicelist = list(devices_set.intersection(devicelist_set))
            else:
                returndevicelist = querydevicelist

            returndevice = dict()
            retbody['devices'] = returndevice
#            logger.debug("returndevicelist = %r" %(returndevicelist))
            for deviceid in returndevicelist:
                lastdatakey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid) 
                lastdatadict = self._redis.hgetall(lastdatakey)
                deviceinfo = list()
                returndevice[deviceid] = deviceinfo
                if lastdatadict is None:
                    continue
                queryinterfacelist = devices[deviceid]
                currentinterfacelist = lastdatadict.keys()
                returninterfacelist = list(set(queryinterfacelist).intersection(set(currentinterfacelist)))
#                logger.debug("==queryinterfacelist = %r" %(queryinterfacelist))
#                logger.debug("==currentinterfacelist = %r" %(currentinterfacelist))
#                logger.debug("==returninterfacelist = %r" %(returninterfacelist))
                for key in returninterfacelist:
                    interfaceinfo = dict()
                    interfaceinfo = eval(lastdatadict[key])
                    if 'recordtime' in interfaceinfo:
                        interfaceinfo[key]['recordtime'] = interfaceinfo[key]['recordtime'].__str__()

                    deviceinfo.append(interfaceinfo)

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
    if _config is not None and 'member' in _config and _config['member'] is not None:
        if 'thread_count' in _config['member'] and _config['member']['thread_count'] is not None:
            thread_count = int(_config['member']['thread_count'])

    for i in xrange(0, thread_count):
        obj = QueryLocgic(i)
        obj.setDaemon(True)
        obj.start()

    while 1:
        time.sleep(1)
