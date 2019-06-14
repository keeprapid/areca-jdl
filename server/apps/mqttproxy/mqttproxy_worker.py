#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import paho.mqtt.client as mqtt

from multiprocessing import Process
import redis
import sys
import time
import uuid
import hashlib
import json
import threading
import os
import logging
import logging.config
import datetime
import pymongo
import random

logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')
if '/opt/Keeprapid/Areca/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Areca/server/apps/common')
import workers



class MqttProxy(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("MqttProxy :running in __init__")

        fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/mqtt.conf", "r")
        self._mqttconfig = json.load(fileobj)
        fileobj.close()

        self.thread_index = thread_index
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.recv_queue_name = "W:Queue:MqttProxy"
        if 'mqttproxy' in _config:
            if 'Consumer_Queue_Name' in _config['mqttproxy']:
                self.recv_queue_name = _config['mqttproxy']['Consumer_Queue_Name']

#        self.publish_queue_name = "W:Queue:MQTTPub"
#        if 'mqtt_publish' in _config:
#            if 'Consumer_Queue_Name' in _config['mqtt_publish']:
#                self.publish_queue_name = _config['mqtt_publish']['Consumer_Queue_Name']



    def run(self):
        logger.debug("Start DataCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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


    def getFloatfromBuf(self, buf):
        index = buf.find('.')
        if index == -1:
            return 0,0

        endindex = index+3
        return endindex,float(buf[:endindex])


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
        msgfrom = ''
        if "from" in msgdict and msgdict['from'] is not None:
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
        logger.debug('action_cmd : %s' % (action_cmd))
        action_version = msg_in['version']
        logger.debug('action_version : %s' % (action_version))
        if 'body' in msg_in:
            action_body = msg_in['body']
#            logger.debug('action_body : %s' % (action_body))
        else:
            action_body = None
            logger.debug('no action_body')

        if action_cmd == 'mqtt_register':
            self._proc_action_mqtt_register(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_mqtt_register(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_mqtt_register action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('topic_name' not in action_body) or  ('interval' not in action_body):
                ret['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['topic_name'] is None or action_body['topic_name'] == '':
                ret['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['interval'] is None or action_body['interval'] == '':
                ret['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            topic_name = action_body['topic_name']
            interval = int(action_body['interval'])
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',topic_name,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                memberinfo = self._redis.hgetall(resultlist[0])

                clientonlinekey = self.KEY_CLIENT_ONLINE_FLAG % (topic_name, memberinfo.get('_id'))
                if self._redis.exists(clientonlinekey) is False:
                    #如果appclient 是之前超时的话就发送缓存消息，尚不实现
                    pass

                self._redis.set(clientonlinekey,datetime.datetime.now().__str__())
                self._redis.expire(clientonlinekey, interval+10)

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
    if _config is not None and 'mqttproxy' in _config and _config['mqttproxy'] is not None:
        if 'thread_count' in _config['mqttproxy'] and _config['mqttproxy']['thread_count'] is not None:
            thread_count = int(_config['mqttproxy']['thread_count'])

    for i in xrange(0, thread_count):
        logic = MqttProxy(i)
        logic.setDaemon(True)
        logic.start()

    while 1:
        time.sleep(1)


