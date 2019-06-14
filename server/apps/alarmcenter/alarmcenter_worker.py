#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  alarmcenter_worker.py
# creator:   jacob.qian
# datetime:  2017-4-20
# 告警业务逻辑

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
import datetime
from bson.objectid import ObjectId
if '/opt/Keeprapid/Areca/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Areca/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')


class AlarmCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(AlarmCenter, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("AlarmCenter :running in __init__")

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
        self.recv_queue_name = "W:Queue:AlarmCenter"
        if 'alarmcenter' in _config:
            if 'Consumer_Queue_Name' in _config['alarmcenter']:
                self.recv_queue_name = _config['alarmcenter']['Consumer_Queue_Name']

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
        self.dataconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbdata = self.dataconn.datacenter
        self.collect_alarms = self.dbdata.alarms


    def run(self):
        logger.debug("Start AlarmCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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

        if action_cmd == 'push_alarm_list':
            self._proc_action_push_alarm_list(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return


    def _proc_action_push_alarm_list(self, version, action_body, retdict, retbody):
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

        logger.debug(" into _proc_action_push_alarm_list action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('alarmlist' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['alarmlist'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            insertdatalist =list()
            alarmlist = action_body['alarmlist']
            for alarminfo in alarmlist:
                #AD2DI4报警一天只产生一次
                alarmtype = alarminfo.get("alarm_type")
                if alarmtype == self.ALARM_TYPE_DEVICE_MAINTENANCE:
                    deviceid = alarminfo.get("deviceid")
                    daystr = datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%d")
                    alarmdayflag = self.KEY_DI4AD2_ALARM_DAY_FLAG % (deviceid,daystr)
                    if self._redis.exists(alarmdayflag) is True:
                        logger.error("<==Maintance alarm key [%s] exists!!" % (alarmdayflag))
                        continue
                    else:
                        self._redis.set(alarmdayflag,'1')
                        self._redis.expire(alarmdayflag, 24*60*60)
                        logger.error("==>Set Maintance alarm key [%s]!!" % (alarmdayflag))

                alarmlevel = alarminfo['level']
                memberlist = list()
                #获取设备所属用户id
                if alarmlevel == self.ALARM_LEVEL_VDEVICE:
                    vdeviceid = alarminfo['vdeviceid']
                    appid = alarminfo['appid']
                    key = self.KEY_APPID_VDID % (appid,vdeviceid)
                    vdeviceinfo = self._redis.hgetall(key)
                    if vdeviceinfo is not None:
                        mlist = vdeviceinfo.get('member_id')
                        if mlist is not None:
                            memberlist = eval(mlist)
                        else:
                            memberlist.append(vdeviceinfo.get('ownerid'))
                else:
                    deviceid = alarminfo['deviceid']
                    objectid = alarminfo['objectid']
                    key = self.KEY_IMEI_ID % (deviceid, objectid)
                    deviceinfo = self._redis.hgetall(key)
                    if deviceinfo is not None:
                        mlist = deviceinfo.get('member_id')
                        if mlist is not None:
                            memberlist = eval(mlist)
                        else:
                            memberlist.append(deviceinfo.get('ownerid'))
                for memberid in memberlist:                  
                    notifydict = dict()
                    insertdata = dict()
                    insertdata.update(alarminfo)
                    insertdata['datetime'] = datetime.datetime.now()
                    insertdata['read_state'] = 'unread'
                    insertdata['operation'] = ''
                    insertdata['memberid'] = memberid
                    insertdatalist.append(insertdata)

                    notifydict.update(alarminfo)
                    notifydict['memberid'] = memberid
                    notifydict['datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    msgdict = dict()
                    msgdict['action_cmd'] = 'send_warning_msg'
                    msgdict['body'] = notifydict
                    msgdict['sockid'] = ''
                    msgdict['from'] = ''
                    msgdict['version'] = '1.0'
                    if 'wxagentareca' in self._config:
                        self._redis.lpush(self._config['wxagentareca']['Consumer_Queue_Name'],json.dumps(msgdict))

                    if alarmlevel == self.ALARM_LEVEL_INTERFACE:
                        searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', memberid)
                        resultlist = self._redis.keys(searchkey)
    #                    logger.debug(resultlist)
                        if len(resultlist):
                            username = resultlist[0].split(':')[2]
                            senddict = dict()
                            senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                            tmpdict = dict()
                            tmpdict['action_cmd'] = 'interface_alarminfo'
                            tmpdict['seq_id'] = 1
                            tmpdict['version'] = '1.0'
                            tmpbody = dict()
                            tmpdict['body'] = tmpbody
                            tmpbody.update(alarminfo)
                            tmpbody['utctime'] = int(time.time())
                            senddict['sendbuf'] = json.dumps(tmpdict)
                            #发送消息
                            if 'mqtt_publish' in self._config:
                                self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))

            if len(insertdatalist):
                self.collect_alarms.insert_many(insertdatalist)
            retdict['error_code'] = '200'
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


if __name__ == "__main__":
    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()

    thread_count = 1
    if _config is not None and 'alarmcenter' in _config and _config['alarmcenter'] is not None:
        if 'thread_count' in _config['alarmcenter'] and _config['alarmcenter']['thread_count'] is not None:
            thread_count = int(_config['alarmcenter']['thread_count'])

    for i in xrange(0, thread_count):
        obj = AlarmCenter(i)
        obj.setDaemon(True)
        obj.start()

    while 1:
        time.sleep(1)
