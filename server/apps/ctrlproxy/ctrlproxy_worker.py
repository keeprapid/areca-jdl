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


class CtrlProxy(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(CtrlProxy, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("CtrlProxy :running in __init__")
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
        if 'ctrlproxy' in _config:
            if 'Consumer_Queue_Name' in _config['ctrlproxy']:
                self.recv_queue_name = _config['ctrlproxy']['Consumer_Queue_Name']

#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.db = self.mongoconn.member
        self.collect_memberinfo = self.db.memberinfo
        self.collect_memberlog = self.db.memberlog
#        self.deviceconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.deviceconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.deviceconn.device
        self.collect_gearinfo = self.dbgear.device_info
        self.collect_gearlog = self.dbgear.device_log
        self.collect_template = self.dbgear.device_template


    def run(self):
        logger.debug("Start CtrlProxy pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
#        try:
        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                logger.debug("_proc_message cost %f" % (time.time()-t1))
                    
#        except Exception as e:
#            logger.debug("%s except raised : %s " % (e.__class__, e.args))



    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
        logger.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
            logger.debug(recvbuf)
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
        logger.debug(msg_resp)
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

        if action_cmd == 'device_ctrl':
            self._proc_action_device_ctrl(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'ctrl_report':
            self._proc_action_ctrl_report(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_device_ctrl(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_device_ctrl action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('deviceid' not in action_body) or  ('tid' not in action_body) or ('vid' not in action_body) or ('ctrlcontent' not in action_body): 
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['deviceid'] is None or action_body['deviceid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['tid'] is None or action_body['tid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            if action_body['vid'] is None or action_body['vid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return
            if action_body['ctrlcontent'] is None or action_body['ctrlcontent'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return
#            if action_body['ctrlid'] is None or action_body['ctrlid'] == '':
#                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
#                return

            recv_deviceid = action_body['deviceid']
            tid = action_body['tid']
            ctrlcontent = action_body['ctrlcontent']
            vid = action_body['vid']
            ctrlid = ''
            if 'ctrlid' in action_body:
                ctrlid = action_body.get('ctrlid')

            searchkey = self.KEY_TOKEN_NAME_ID % (tid,'*','*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            memberinfo = self._redis.hgetall(resultlist[0])
            deviceidlist = list()
            if isinstance(recv_deviceid,list):
                deviceidlist = recv_deviceid
            else:
                deviceidlist.append(recv_deviceid)

            for deviceid in deviceidlist:
                if memberinfo['username'] != 'admin':
                    member_devicelist = eval(memberinfo['device'])
                    if deviceid not in member_devicelist:
                        if len(deviceidlist) == 1:
                            retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                            return
                        else:
                            continue

                sendbuf = "$%%%%%s$" % (ctrlcontent)
                cmddict = dict()
                cmddict['sendbuf'] = sendbuf
                cmddict['ctrlid'] = ctrlid
                sockkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid,'*')
                resultlist = self._redis.keys(sockkey)
                if len(resultlist) == 0:
                    #缓存命令
                    logger.error("device[%s] has no sockinfo, cache the cmd [%s] now ctrlid [%s]" %(deviceid, sendbuf, ctrlid))
                    retdict['error_code'] = self.ERRORCODE_IMEI_STATE_OOS
                    queuename = self.KEY_DEVICE_CTRL_CMD_LIST%(deviceid)
                    self._redis.lpush(queuename,cmddict)
                    self._redis.expire(queuename,self.CTRL_QUEUE_TIMEOUT)
                    #上报状态
                    msgdict = dict()
                    msgdict['action_cmd'] = 'ctrl_report'
                    msgdict['body'] = dict()
                    msgdict['body']['report'] = 'queue'
                    msgdict['body']['content'] = sendbuf
                    msgdict['body']['deviceid'] = deviceid
                    msgdict['body']['ctrlid'] = ctrlid
                    msgdict['sockid'] = ''
                    msgdict['from'] = ''
                    msgdict['version'] = '1.0'
                    if 'ctrlproxy' in self._config:
                        self._redis.lpush(self._config['ctrlproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                else:
                    #先看是否当前已经有条命令了
                    currentcmdkey = self.KEY_DEVICE_CURRENT_CMD % (deviceid)
                    if self._redis.exists(currentcmdkey):
                        queuename = self.KEY_DEVICE_CTRL_CMD_LIST%(deviceid)
                        self._redis.lpush(queuename,cmddict)
                        self._redis.expire(queuename,self.CTRL_QUEUE_TIMEOUT)
                        msgdict = dict()
                        msgdict['action_cmd'] = 'ctrl_report'
                        msgdict['body'] = dict()
                        msgdict['body']['report'] = 'queue'
                        msgdict['body']['content'] = sendbuf
                        msgdict['body']['deviceid'] = deviceid
                        msgdict['body']['ctrlid'] = ctrlid
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'ctrlproxy' in self._config:
                            self._redis.lpush(self._config['ctrlproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                    else:                    
                        sockinfo = self._redis.get(resultlist[0])
                        retdict['error_code'] = self.ERRORCODE_OK
                        logger.error("device[%s] has sockid[%s], cache and send [%s] now, wait for respones" %(deviceid, sockinfo, sendbuf))
                        msgdict = dict()
                        msgdict['action_cmd'] = 'device_ctrl'
                        msgdict['body'] = dict()
                        msgdict['body']['sendbuf'] = sendbuf
                        msgdict['body']['sockid'] = sockinfo
                        msgdict['body']['deviceid'] = deviceid
                        msgdict['body']['ctrlid'] = ctrlid
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'deviceproxy' in self._config:
                            self._redis.lpush(self._config['deviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                        currentcmdkey = self.KEY_DEVICE_CURRENT_CMD % (deviceid)
                        self._redis.set(currentcmdkey, cmddict)
                        self._redis.expire(currentcmdkey,self.CTRL_CMD_TIMEOUT)
                    
            

            retdict['error_code'] = self.ERRORCODE_OK
            
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL



    def _proc_action_ctrl_report(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_ctrl_report action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('deviceid' not in action_body) or  ('report' not in action_body) or ('content' not in action_body) or ('ctrlid' not in action_body): 
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['deviceid'] is None or action_body['deviceid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['report'] is None or action_body['report'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            if action_body['content'] is None or action_body['content'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return
            if action_body['ctrlid'] is None or action_body['ctrlid'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return

            deviceid = action_body['deviceid']
            report = action_body['report']
            content = action_body['content']
            ctrlid = action_body['ctrlid']
            queuelen = 0
            queuename = self.KEY_DEVICE_CTRL_CMD_LIST%(deviceid)
            if self._redis.exists(queuename):
                queuelen = self._redis.llen(queuename)

            deviceinfo = self.collect_gearinfo.find_one({'deviceid':deviceid})
            if deviceinfo is None:
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return
            memberidlist = deviceinfo.get('member_id')
            for memberid in memberidlist:
                searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', memberid)
                resultlist = self._redis.keys(searchkey)
                logger.debug(resultlist)
                if len(resultlist):
                    username = resultlist[0].split(':')[2]
                    senddict = dict()
                    senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                    tmpdict = dict()
                    tmpdict['action_cmd'] = 'ctrl_report'
                    tmpdict['seq_id'] = 1
                    tmpdict['version'] = '1.0'
                    tmpdict['from'] = ''
                    tmpdict['sockid'] = ''
                    tmpbody = dict()
                    tmpdict['body'] = tmpbody
                    tmpbody['deviceid'] = deviceid
                    tmpbody['content'] = content
                    tmpbody['ctrlid'] = ctrlid
                    tmpbody['report'] = report
                    tmpbody['queuelen'] = queuelen
                    tmpbody['utctime'] = int(time.time())
                    logger.debug(senddict)
                    senddict['sendbuf'] = json.dumps(tmpdict)
                    #发送消息
                    if 'mqtt_publish' in self._config:
                        self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

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
    if _config is not None and 'ctrlproxy' in _config and _config['ctrlproxy'] is not None:
        if 'thread_count' in _config['ctrlproxy'] and _config['ctrlproxy']['thread_count'] is not None:
            thread_count = int(_config['ctrlproxy']['thread_count'])

    for i in xrange(0, thread_count):
        p = CtrlProxy(i)
        p.setDaemon(True)
        p.start()

    while 1:
        time.sleep(1)
