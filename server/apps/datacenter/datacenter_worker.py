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
import math
from bson.objectid import ObjectId
from bson import json_util

if '/opt/Keeprapid/Areca/server/apps/common' not in sys.path:
    sys.path.append('/opt/Keeprapid/Areca/server/apps/common')
import workers

logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')


class DataCenter(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(DataCenter, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("DataCenter :running in __init__")

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
        if 'datacenter' in _config:
            if 'Consumer_Queue_Name' in _config['datacenter']:
                self.recv_queue_name = _config['datacenter']['Consumer_Queue_Name']

        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
#        self.conn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.conn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.datadb = self.conn.datacenter
        self.collect_data = self.datadb.data_info

#        self.deviceconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.deviceconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbgear = self.deviceconn.device
        self.collect_gearinfo = self.dbgear.device_info
#        self.collect_vdeviceinfo = self.dbgear.vdevice_info
#        self.ctrlconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.ctrlconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbctrl = self.ctrlconn.control
        self.collect_ctrl = self.dbctrl.control_log

    def run(self):
        logger.debug("Start DataCenter pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
        try:
#        if 1:
            while 1:
                recvdata = self._redis.brpop(self.recv_queue_name)
                t1 = time.time()
                if recvdata:
                    self._proc_message(recvdata[1])
                # logger.debug("_proc_message cost %f" % (time.time()-t1))                    
                    
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))


    def getFloatfromBuf(self, buf):
        index = buf.find('.')
        if index == -1:
            return len(buf),int(buf)

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

        if action_cmd == 'device_upmsg':
            self._proc_action_device_upmsg(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_ctrlmsg_resp(self,recvedbuf):
        #在消息解码之前先判断是不是控制信息的返回
        recvelist = recvedbuf.split(',')
        if len(recvelist)>0:
            deviceid = recvelist.pop(0)
            recvedict = dict()
            cmdmsgstart = recvedbuf.find('CMD_MSG:')
            if cmdmsgstart != -1:
                cmdmsgend = recvedbuf.find('",')
                cmdmsgtmp = recvedbuf[cmdmsgstart:cmdmsgend+1]
                kvtmp = cmdmsgtmp.split(':')
                recvedict[kvtmp[0].lower()] = ':'.join(kvtmp[1:])
                tpmrecvedbuf = recvedbuf.replace(cmdmsgtmp,'')
                recvelist = tpmrecvedbuf.split(',')

            for splitstr in recvelist:
                kvlist = splitstr.split(':')
                recvedict[kvlist[0].lower()] = ':'.join(kvlist[1:])
#                logger.debug(recvedict)
            cachedcmdlistkey = self.KEY_DEVICE_CTRL_CMD_LIST % (deviceid)
            currentcmdkey = self.KEY_DEVICE_CURRENT_CMD % (deviceid)
            #第一条消息入库，准备发送第二条消息
#                cachecmd = self._redis.rpop(cachedcmdlistkey)
            cachecmd = self._redis.get(currentcmdkey)
            if cachecmd is not None:
                cachecmddict = eval(cachecmd)
                if 'cmd_msg' in recvedict and 'sendbuf' in cachecmddict:
                    cmd_msg = recvedict['cmd_msg']
                    cmd_msg = cmd_msg.replace('%','').replace('"','')
                    tmpcachemsg = cachecmddict['sendbuf'].replace('%','').replace('$','')
                    logger.debug("cmd_msg = %s,tmpcachemsg=%s",cmd_msg,tmpcachemsg)
                    if cmd_msg != tmpcachemsg:
                        logger.debug(" %s unmatch %s wait till timeout",cmd_msg,tmpcachemsg)
#                            self._redis.rpush(cachedcmdlistkey,cachecmd)
                    else:
                        msgdict = dict()
                        msgdict['action_cmd'] = 'ctrl_report'
                        msgdict['body'] = dict()
                        msgdict['body']['report'] = 'finish'
                        msgdict['body']['content'] = tmpcachemsg
                        msgdict['body']['deviceid'] = deviceid
                        msgdict['body']['recvbuf'] = recvedbuf
                        msgdict['body']['ctrlid'] = cachecmddict.get('ctrlid')
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'ctrlproxy' in self._config:
                            self._redis.lpush(self._config['ctrlproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                        self._redis.delete(currentcmdkey)

            insertdata = dict()
            insertdata['deviceid'] = deviceid
            insertdata['cmd_result'] = recvedict.get('cmd_result')
            insertdata['cmd_msg'] = recvedict.get('cmd_msg')
            timestamp = datetime.datetime.now()
            if 'time' in recvedict:
                if recvedict['time'].startswith('00/00') is False:
                    timestr = '20%s'%recvedict['time']
    #                       logger.debug(timestr)
                    timestamp = datetime.datetime.strptime(timestr,'%Y/%m/%d %H:%M')

            insertdata['timestamp'] = timestamp
            self.collect_ctrl.insert_one(insertdata)
            #如果当前没有已发送的命令，就继续往下发送后续命令
            if self._redis.exists(currentcmdkey) is False:
                sockkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid,'*')
                resultlist = self._redis.keys(sockkey)
                if len(resultlist):
                    nextcmd = self._redis.rpop(cachedcmdlistkey)
                    if nextcmd is not None:
                        nextcmddict = eval(nextcmd)
                        sockinfo = self._redis.get(resultlist[0])
                        msgdict = dict()
                        msgdict['action_cmd'] = 'device_ctrl'
                        msgdict['body'] = dict()
                        msgdict['body']['sendbuf'] = nextcmddict['sendbuf']
                        msgdict['body']['sockid'] = sockinfo
                        msgdict['body']['deviceid'] = deviceid
                        msgdict['body']['ctrlid'] = nextcmddict['ctrlid']
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'deviceproxy' in self._config:
                            self._redis.lpush(self._config['deviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                        self._redis.set(currentcmdkey, nextcmddict)
                        self._redis.expire(currentcmdkey,self.CTRL_CMD_TIMEOUT)

                    self._redis.expire(cachedcmdlistkey,self.CTRL_QUEUE_TIMEOUT)     
                    
    def _proc_action_device_upmsg(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_device_upmsg action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('recvedbuf' not in action_body) or  ('sockid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['recvedbuf'] is None or action_body['recvedbuf'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['sockid'] is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return


            sockid = action_body['sockid']
            recvedbuf = action_body['recvedbuf']
            #区分是否是控制的返回消息
            if recvedbuf.find('CMD_RESULT')>=0:
                self._proc_ctrlmsg_resp(recvedbuf)
                return

#             if recvedbuf.find('CMD_RESULT')>=0:
# #                logger.debug("=====Find CMD_RESULT====")
#                 recvelist = recvedbuf.split(',')
#                 deviceid = recvelist.pop(0)
#                 recvedict = dict()
#                 cmdmsgstart = recvedbuf.find('CMD_MSG:')
#                 if cmdmsgstart != -1:
#                     cmdmsgend = recvedbuf.find('",')
#                     cmdmsgtmp = recvedbuf[cmdmsgstart:cmdmsgend+1]
#                     kvtmp = cmdmsgtmp.split(':')
#                     recvedict[kvtmp[0].lower()] = ':'.join(kvtmp[1:])
#                     tpmrecvedbuf = recvedbuf.replace(cmdmsgtmp,'')
#                     recvelist = tpmrecvedbuf.split(',')

#                 for splitstr in recvelist:
#                     kvlist = splitstr.split(':')
#                     recvedict[kvlist[0].lower()] = ':'.join(kvlist[1:])
# #                logger.debug(recvedict)
#                 cachedcmdlistkey = self.KEY_DEVICE_CTRL_CMD_LIST % (deviceid)
#                 currentcmdkey = self.KEY_DEVICE_CURRENT_CMD % (deviceid)
#                 #第一条消息入库，准备发送第二条消息
# #                cachecmd = self._redis.rpop(cachedcmdlistkey)
#                 cachecmd = self._redis.get(currentcmdkey)
#                 if cachecmd is not None:
#                     cachecmddict = eval(cachecmd)
#                     if 'cmd_msg' in recvedict and 'sendbuf' in cachecmddict:
#                         cmd_msg = recvedict['cmd_msg']
#                         cmd_msg = cmd_msg.replace('%','').replace('"','')
#                         tmpcachemsg = cachecmddict['sendbuf'].replace('%','').replace('$','')
#                         logger.debug("cmd_msg = %s,tmpcachemsg=%s",cmd_msg,tmpcachemsg)
#                         if cmd_msg != tmpcachemsg:
#                             logger.debug(" %s unmatch %s wait till timeout",cmd_msg,tmpcachemsg)
# #                            self._redis.rpush(cachedcmdlistkey,cachecmd)
#                         else:
#                             msgdict = dict()
#                             msgdict['action_cmd'] = 'ctrl_report'
#                             msgdict['body'] = dict()
#                             msgdict['body']['report'] = 'finish'
#                             msgdict['body']['content'] = tmpcachemsg
#                             msgdict['body']['deviceid'] = deviceid
#                             msgdict['body']['ctrlid'] = cachecmddict.get('ctrlid')
#                             msgdict['sockid'] = ''
#                             msgdict['from'] = ''
#                             msgdict['version'] = '1.0'
#                             if 'ctrlproxy' in self._config:
#                                 self._redis.lpush(self._config['ctrlproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
#                             self._redis.delete(currentcmdkey)

#                 insertdata = dict()
#                 insertdata['deviceid'] = deviceid
#                 insertdata['cmd_result'] = recvedict.get('cmd_result')
#                 insertdata['cmd_msg'] = recvedict.get('cmd_msg')
#                 timestamp = datetime.datetime.now()
#                 if 'time' in recvedict:
#                     if recvedict['time'].startswith('00/00') is False:
#                         timestr = '20%s'%recvedict['time']
# #                       logger.debug(timestr)
#                         timestamp = datetime.datetime.strptime(timestr,'%Y/%m/%d %H:%M')

#                 insertdata['timestamp'] = timestamp
#                 self.collect_ctrl.insert_one(insertdata)
#                 #如果当前没有已发送的命令，就继续往下发送后续命令
#                 if self._redis.exists(currentcmdkey) is False:
#                     sockkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid,'*')
#                     resultlist = self._redis.keys(sockkey)
#                     if len(resultlist):
#                         nextcmd = self._redis.rpop(cachedcmdlistkey)
#                         if nextcmd is not None:
#                             nextcmddict = eval(nextcmd)
#                             sockinfo = self._redis.get(resultlist[0])
#                             msgdict = dict()
#                             msgdict['action_cmd'] = 'device_ctrl'
#                             msgdict['body'] = dict()
#                             msgdict['body']['sendbuf'] = nextcmddict['sendbuf']
#                             msgdict['body']['sockid'] = sockinfo
#                             msgdict['body']['deviceid'] = deviceid
#                             msgdict['body']['ctrlid'] = nextcmddict['ctrlid']
#                             msgdict['sockid'] = ''
#                             msgdict['from'] = ''
#                             msgdict['version'] = '1.0'
#                             if 'deviceproxy' in self._config:
#                                 self._redis.lpush(self._config['deviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
#                             self._redis.set(currentcmdkey, nextcmddict)
#                             self._redis.expire(currentcmdkey,self.CTRL_CMD_TIMEOUT)

#                         self._redis.expire(cachedcmdlistkey,self.CTRL_QUEUE_TIMEOUT)
#                 return



            msglen = len(recvedbuf)
            if msglen < 12:
                retdict['error_code'] = 200
                return

            index = 0
            deviceid = recvedbuf[index:index+8]

            if sockid != "":
                searchkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid, '*')
                resultlist = self._redis.keys(searchkey)
#                logger.debug("resultlist = %r" % (resultlist))
                devicesocketkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid,sockid)
#                logger.debug("devicesocketkey = %s" % (devicesocketkey))
                if len(resultlist):
                    if devicesocketkey in resultlist:
                        resultlist.remove(devicesocketkey)

                    for key in resultlist:
                        lastsockid = key.split(':')[3]
                        msgdict = dict()
                        msgdict['action_cmd'] = 'delete_sock'
                        msgdict['body'] = dict()
                        msgdict['body']['sockid'] = lastsockid
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'deviceproxy' in self._config:
                            self._redis.lpush(self._config['deviceproxy']['Consumer_Queue_Name_TCP'],json.dumps(msgdict))

                        

                self._redis.set(devicesocketkey,sockid)
            
            index += 8

            searchkey = self.KEY_IMEI_ID % (deviceid, '*')
            resultlist = self._redis.keys(searchkey)
#            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_IMEI_NOT_EXIST
                return

            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            notifymsglist = list()
            notifygpslist = list()

            cwdtype = recvedbuf[index:index+4]
            index+=4
            timestr = recvedbuf[index:index+14]
            index+=14
            if timestr.startswith('00/00'):
                timestamp = datetime.datetime.now()
            else:
                timestr = '20%s'%timestr
#                logger.debug(timestr)
                timestamp = datetime.datetime.strptime(timestr,'%Y/%m/%d %H:%M')
#            logger.debug(timestamp)

            recvtimestamp = datetime.datetime.now()
            #计算设备状态放到Transaction模块中去了，不在此计算了

#            #设置online标志
#            onlinekey = self.KEY_DEVICE_ONLINE_FLAG % (deviceid)
#            timeout = self.GEAR_ONLINE_TIMEOUT
#            if 'd_timeout' in imeiinfo:
#                timeout = int(imeiinfo['d_timeout'])
#            if self._redis.exists(onlinekey):
#                self._redis.expire(onlinekey,timeout)
#            else:
#                #如果之前没有在线标记，那么在这里设置在线状态（正常，或者告警)
#                self._redis.set(onlinekey, recvtimestamp.__str__())
#                self._redis.expire(onlinekey,timeout)
#                alarmkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid, '*', '*')
#                resultlist = self._redis.keys(alarmkey)
#                newstate = self.GEAR_STATE_ACTIVE
#                if len(resultlist):
#                    newstate = self.GEAR_STATE_ALARM
#                senddict = dict()
#                senddict['action_cmd'] = 'update_device_state'
#                senddict['seq_id'] = 1
#                senddict['version'] = '1.0'
#                sendbody = dict()
#                senddict['body'] = sendbody
#                sendbody['deviceid'] = deviceid
#                sendbody['state'] = newstate
#                #发送消息
#                if 'transaction' in self._config:
#                    self._sendMessage(self._config['transaction']['Consumer_Queue_Name'], json.dumps(senddict))
#
#
#            #设置app vdid的online标志
#            searchkey = self.KEY_APPID_VDID_DID % ('*','*',deviceid)
#            resultlist = self._redis.keys(searchkey)
#            for avdkey in resultlist:
#                klist = avdkey.split(':')
#                appid = klist[2]
#                vdeviceid = klist[3]
#                #如果之前没有在线标记，那么在这里设置在线状态（正常，或者告警)
#                setkey = self.KEY_ONLINE_APPID_VDID % (appid, vdeviceid)
#                if self._redis.exists(setkey):
#                    self._redis.expire(setkey,self.GEAR_ONLINE_TIMEOUT)
#                else:
#                    self._redis.set(setkey, recvtimestamp.__str__())
#                    self._redis.expire(setkey,self.GEAR_ONLINE_TIMEOUT)
#                    alarmkey = self.KEY_ALARM_APPID_VDID % (appid, vdeviceid)
#                    newstate = self.GEAR_STATE_ACTIVE
#                    if self._redis.exists(alarmkey):
#                        newstate = self.GEAR_STATE_ALARM
#                    senddict = dict()
#                    senddict['action_cmd'] = 'update_vdevice_state'
#                    senddict['seq_id'] = 1
#                    senddict['version'] = '1.0'
#                    sendbody = dict()
#                    senddict['body'] = sendbody
#                    sendbody['vdeviceid'] = vdeviceid
#                    sendbody['appid'] = appid
#                    sendbody['state'] = newstate
#                    #发送消息
#                    if 'transaction' in self._config:
#                        self._sendMessage(self._config['transaction']['Consumer_Queue_Name'], json.dumps(senddict))
#

            #先找到GPS信息
            hasgps = False
            gpsinfo = dict()
            gpsidx = recvedbuf.find('GPS:')
            if gpsidx != -1:
                updatedict = dict()
                hasgps = True
                gpsbuf = recvedbuf[gpsidx:]
                gpslist = gpsbuf.replace('GPS','').replace(':','').replace(' ','').split(',')
                for i in xrange(0,len(gpslist)):
                    gpsstr = gpslist[i].replace(' ','')
                    if i == 1:
                        updatedict['last_lat'] = gpsstr
                        if gpsstr[0] in ['N','S']:
#                                insertdata['latitude_type'] = gpsstr[0]
                            tmplat = float(gpsstr[1:])
                            if gpsstr[0] == 'N':
                                gpsinfo['latitude_type'] = 'N'
                                gpsinfo['latitude_value'] = str(math.fabs(tmplat))
                                updatedict['last_lat'] = str(math.fabs(tmplat))
                            else:
                                gpsinfo['latitude_type'] = 'S'
                                gpsinfo['latitude_value'] = str(-math.fabs(tmplat))
                                updatedict['last_lat'] = str(-math.fabs(tmplat))
#                                gpsinfo['latitude_value'] = gpsstr[1:]
                        else:
#                                gpsinfo['latitude_type'] = '+'
                            tmplat = float(gpsstr)
                            if tmplat >= 0:
                                gpsinfo['latitude_type'] = 'N'
                                gpsinfo['latitude_value'] = str(tmplat)
                                updatedict['last_lat'] = str(tmplat)
                            else:
                                gpsinfo['latitude_type'] = 'S'
                                gpsinfo['latitude_value'] = str(tmplat)
                                updatedict['last_lat'] = str(tmplat)

#                                gpsinfo['latitude_value'] = gpsstr
                    elif i == 0:
#                            updatedict['last_long'] = gpsstr
                        if gpsstr[0] in ['E','W']:
                            tmplng = float(gpsstr[1:])
                            if gpsstr[0] == 'E':
                                gpsinfo['longitude_type'] = 'E'
                                gpsinfo['longitude_value'] = str(math.fabs(tmplng))
                                updatedict['last_long'] = str(math.fabs(tmplng))
                            else:
                                gpsinfo['longitude_type'] = 'W'
                                gpsinfo['longitude_value'] = str(-math.fabs(tmplng))
                                updatedict['last_long'] = str(-math.fabs(tmplng))
#                                gpsinfo['longitude_type'] = gpsstr[0]
#                                gpsinfo['longitude_value'] = gpsstr[1:]
                        else:
                            tmplng = float(gpsstr)
                            if tmplng >=0:
                                gpsinfo['longitude_type'] = 'E'
                                gpsinfo['longitude_value'] = str(tmplng)
                                updatedict['last_long'] =  str(tmplng)
                            else:
                                gpsinfo['longitude_type'] = 'W'
                                gpsinfo['longitude_value'] = str(tmplng)
                                updatedict['last_long'] =  str(tmplng)
#                                gpsinfo['longitude_type'] = '+'
#                                gpsinfo['longitude_value'] = gpsstr
#                    if i == 1:
#                        updatedict['last_lat'] = gpsstr
#                        if gpsstr[0] in ['N','S']:
#                            gpsinfo['latitude_type'] = gpsstr[0]
#                            gpsinfo['latitude_value'] = gpsstr[1:]
#                        else:
#                            gpsinfo['latitude_type'] = '+'
#                            gpsinfo['latitude_value'] = gpsstr
#                    elif i == 0:
#                        updatedict['last_long'] = gpsstr
#                        if gpsstr[0] in ['E','W']:
#                            gpsinfo['longitude_type'] = gpsstr[0]
#                            gpsinfo['longitude_value'] = gpsstr[1:]
#                        else:
#                            gpsinfo['longitude_type'] = '+'
#                            gpsinfo['longitude_value'] = gpsstr
                    elif i== 2:
                        gpsinfo['speed'] = gpsstr
                        updatedict['last_vt'] = gpsstr

                gpsnotifydict = dict()
                gpsnotifydict.update(gpsinfo)
                gpsnotifydict['deviceid'] = deviceid
                gpsnotifydict['if_name'] = 'GPS'
                gpsnotifydict['channel'] = 0
                gpsnotifydict['timestamp'] = timestamp
                gpsnotifydict['recv_time'] = recvtimestamp
                notifygpslist.append(gpsnotifydict)
                if len(updatedict):
                    updatedict['last_location_type'] = self.LOCATION_TYPE_GPS
                    updatedict['last_location_time'] = recvtimestamp
                    self._redis.hmset(imeikey, updatedict)
                    self.collect_gearinfo.update_one({'_id':ObjectId(imeiinfo['_id'])},{'$set':updatedict})

            if cwdtype in ['PW00','RP01']:
                interfacename = cwdtype[0:2]
                ch = recvedbuf[index:index+2]
                index+=2
                arm_state = int(recvedbuf[index])
                index+=1
                power_supply = int(recvedbuf[index])
                index+=1
                signal_level = float(recvedbuf[index:index+4])-256
                index+=4
                valuelen,temperature = self.getFloatfromBuf(recvedbuf[index:])
                updatedict = dict()
                updatedict['power_supply'] = power_supply
                updatedict['signal_level'] = signal_level
                updatedict['arm_state'] = arm_state
                updatedict['temperature'] = temperature
#                logger.debug(updatedict)
                self._redis.hmset(imeikey, updatedict)
                self.collect_gearinfo.update_one({'_id':ObjectId(imeiinfo['_id'])},{'$set':updatedict})
                insertdata = dict()
                insertdata['deviceid'] = deviceid
                insertdata['if_name'] = interfacename
                insertdata['power_supply'] = power_supply
                insertdata['signal_level'] = signal_level
                insertdata['arm_state'] = arm_state
                insertdata['temperature'] = temperature
                insertdata['timestamp'] = timestamp
                insertdata['recv_time'] = recvtimestamp

                armdata = dict({\
                    'deviceid':deviceid,\
                    'if_name':'AS',\
                    'channel':'',\
                    'state':arm_state,\
                    'value':arm_state,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(armdata)
                sigdata = dict({\
                    'deviceid':deviceid,\
                    'if_name':'SL',\
                    'channel':'',\
                    'state':0,\
                    'value':signal_level,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(sigdata)
                dcdata = dict({\
                    'deviceid':deviceid,\
                    'if_name':'DC',\
                    'channel':'',\
                    'state':power_supply,\
                    'value':power_supply,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(dcdata)
                armdata1 = dict({\
                    'deviceid':deviceid,\
                    'if_name':'arm_state',\
                    'channel':'',\
                    'state':arm_state,\
                    'value':arm_state,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(armdata1)
                sigdata1 = dict({\
                    'deviceid':deviceid,\
                    'if_name':'signal_level',\
                    'channel':'',\
                    'state':0,\
                    'value':signal_level,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(sigdata1)
                dcdata1 = dict({\
                    'deviceid':deviceid,\
                    'if_name':'power_supply',\
                    'channel':'',\
                    'state':power_supply,\
                    'value':power_supply,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(dcdata1)
#                if hasgps:
#                    insertdata['gpsinfo'] = gpsinfo
                notifymsglist.append(insertdata)
                if cwdtype == 'PW00':
                    deletekey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid)
                    self._redis.delete(deletekey)
#                self.collect_data.insert(insertdata)
                    dcdata1 = dict({\
                        'deviceid':deviceid,\
                        'if_name':'PW',\
                        'channel':'',\
                        'state':0,\
                        'value':0,\
                        'recv_time':recvtimestamp,\
                        'timestamp':timestamp\
                        })
                    notifymsglist.append(dcdata1)
            elif cwdtype in ['ET33','AD22','IC32','EH36']:
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2
                tempbuf = recvedbuf[index:]
                # logger.debug(tempbuf)
                templist = tempbuf.split(',')
                # logger.debug(templist)
                for x in xrange(0,ch):
                    tmpstr = templist.pop(0)
                    # logger.debug(tmpstr)
                    if len(tmpstr):
                        # logger.debug(tmpstr[0])
                        state = int(tmpstr[0])
                        # logger.debug(tmpstr[1:])
                        value = float(tmpstr[1:])
                        insertdata = dict({\
                            'deviceid':deviceid,\
                            'if_name':interfacename,\
                            'channel':x,\
                            'state':state,\
                            'value':value,\
                            'recv_time':recvtimestamp,\
                            'timestamp':timestamp\
                            })
                        if hasgps:
                            insertdata['gpsinfo'] = gpsinfo
                        notifymsglist.append(insertdata)
                        self.collect_data.insert_one(insertdata)
                        #特殊处理 组合温湿度数据信息
                        if 'special_application' in imeiinfo and imeiinfo['special_application'] is not None and imeiinfo['special_application'] != '' and imeiinfo['special_application'] != 'None' and int(imeiinfo['special_application']) == self.SPECIAL_APPLICATION_THBIND and cwdtype in ['ET33','EH36']:
                            combinekey = self.KEY_COMBINATION_DATA_DEVICEID % (deviceid)
                            datakey = '%s%d'%(interfacename,x)
                            self._redis.hset(combinekey,datakey,{'if_name':interfacename,'channel':x,'state':state,'value':value})
                #特殊处理 组合温湿度数据信息,保存打包的数据
                if 'special_application' in imeiinfo and imeiinfo['special_application'] is not None and imeiinfo['special_application'] != '' and imeiinfo['special_application'] != 'None' and int(imeiinfo['special_application']) == self.SPECIAL_APPLICATION_THBIND and cwdtype == 'EH36':
                    combineinfo = self._redis.hgetall(combinekey)
                    datakeylist = ['ET0','ET1','EH0','EH1']
                    combinedatalist = dict()
                    for dkey in datakeylist:
                        if dkey in combineinfo:
                            dinfo = eval(combineinfo[dkey])
                            combinedatalist[dkey] = dinfo
                        else:
                            dinfo = dict()
                            dinfo['if_name'] = dkey[0:2]
                            dinfo['channel'] = int(dkey[-1:])
                            dinfo['state'] = 0
                            dinfo['value'] = 0.0
                            combinedatalist[dkey] = dinfo
                    insertcombinedata = dict()
                    insertcombinedata['deviceid'] = deviceid
                    insertcombinedata['if_name'] = 'CTH'
                    insertcombinedata['channel'] = 0
                    insertcombinedata['state'] = 0
                    insertcombinedata['value'] = 0
                    insertcombinedata['recv_time'] = recvtimestamp
                    insertcombinedata['timestamp'] = timestamp
                    insertcombinedata['datalist'] = combinedatalist
                    self.collect_data.insert_one(insertcombinedata)
            elif cwdtype in ['ET43','AD10','TM11','HU34','GY18','EH57']:
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2

                state = int(recvedbuf[index])
                index +=1
                valuelen,value = self.getFloatfromBuf(recvedbuf[index:])
                index+=valuelen
                insertdata = dict({\
                    'deviceid':deviceid,\
                    'if_name':interfacename,\
                    'channel':ch,\
                    'state':state,\
                    'value':value,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                if hasgps:
                    insertdata['gpsinfo'] = gpsinfo
                notifymsglist.append(insertdata)
                self.collect_data.insert_one(insertdata)             
                if 'special_application' in imeiinfo and  imeiinfo['special_application'] is not None and imeiinfo['special_application'] != '' and imeiinfo['special_application'] != 'None' and int(imeiinfo['special_application']) == self.SPECIAL_APPLICATION_THBIND and cwdtype in ['ET43','EH57']:
                    combinekey = self.KEY_COMBINATION_DATA_DEVICEID % (deviceid)
                    datakey = '%s%d'%(interfacename,x)
                    self._redis.hset(combinekey,datakey,{'if_name':interfacename,'channel':x,'state':state,'value':value})
            elif cwdtype in ['DI02']:
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2

                state = int(recvedbuf[index])
                insertdata = dict({\
                    'deviceid':deviceid,\
                    'if_name':interfacename,\
                    'channel':ch,\
                    'state':state,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                if hasgps:
                    insertdata['gpsinfo'] = gpsinfo
                notifymsglist.append(insertdata)
                self.collect_data.insert_one(insertdata)
            elif cwdtype in ['DI20','DO21']:
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2
                for x in xrange(0,ch):
                    state = int(recvedbuf[index])
                    insertdata = dict({\
                        'deviceid':deviceid,\
                        'if_name':interfacename,\
                        'channel':x,\
                        'state':state,\
                        'recv_time':recvtimestamp,\
                        'timestamp':timestamp\
                        })
                    if hasgps:
                        insertdata['gpsinfo'] = gpsinfo
                    notifymsglist.append(insertdata)
                    self.collect_data.insert_one(insertdata)
                    index+=1
            elif cwdtype in ['GP35']:
                tempbuf = recvedbuf[index:]
                templist = tempbuf.split(":")
                templen = len(templist)
                if templen >= 2:
                    tempbuf = templist[templen-1]
                    insertdata = dict()
                    insertdata['deviceid'] = deviceid
                    insertdata['if_name'] = 'GPS'
                    insertdata['timestamp'] = timestamp
                    insertdata['recv_time'] = recvtimestamp
                    datalist = tempbuf.split(',')
                    datalen = len(datalist)
#                    logger.debug(datalist)
                    updatedict = dict()
                    for i in xrange(0,datalen):
                        gpsstr = datalist[i].replace(' ','')
#                        logger.debug(gpsstr)
                        if i == 1:
                            updatedict['last_lat'] = gpsstr
                            if gpsstr[0] in ['N','S']:
#                                insertdata['latitude_type'] = gpsstr[0]
                                tmplat = float(gpsstr[1:])
                                if gpsstr[0] == 'N':
                                    insertdata['latitude_type'] = 'N'
                                    insertdata['latitude_value'] = str(math.fabs(tmplat))
                                    updatedict['last_lat'] = str(math.fabs(tmplat))
                                else:
                                    insertdata['latitude_type'] = 'S'
                                    insertdata['latitude_value'] = str(-math.fabs(tmplat))
                                    updatedict['last_lat'] = str(-math.fabs(tmplat))
#                                insertdata['latitude_value'] = gpsstr[1:]
                            else:
#                                insertdata['latitude_type'] = '+'
                                tmplat = float(gpsstr)
                                if tmplat >= 0:
                                    insertdata['latitude_type'] = 'N'
                                    insertdata['latitude_value'] = str(tmplat)
                                    updatedict['last_lat'] = str(tmplat)
                                else:
                                    insertdata['latitude_type'] = 'S'
                                    insertdata['latitude_value'] = str(tmplat)
                                    updatedict['last_lat'] = str(tmplat)

#                                insertdata['latitude_value'] = gpsstr
                        elif i == 0:
#                            updatedict['last_long'] = gpsstr
                            if gpsstr[0] in ['E','W']:
                                tmplng = float(gpsstr[1:])
                                if gpsstr[0] == 'E':
                                    insertdata['longitude_type'] = 'E'
                                    insertdata['longitude_value'] = str(math.fabs(tmplng))
                                    updatedict['last_long'] = str(math.fabs(tmplng))
                                else:
                                    insertdata['longitude_type'] = 'W'
                                    insertdata['longitude_value'] = str(-math.fabs(tmplng))
                                    updatedict['last_long'] = str(-math.fabs(tmplng))
#                                insertdata['longitude_type'] = gpsstr[0]
#                                insertdata['longitude_value'] = gpsstr[1:]
                            else:
                                tmplng = float(gpsstr)
                                if tmplng >=0:
                                    insertdata['longitude_type'] = 'E'
                                    insertdata['longitude_value'] = str(tmplng)
                                    updatedict['last_long'] =  str(tmplng)
                                else:
                                    insertdata['longitude_type'] = 'W'
                                    insertdata['longitude_value'] = str(tmplng)
                                    updatedict['last_long'] =  str(tmplng)
#                                insertdata['longitude_type'] = '+'
#                                insertdata['longitude_value'] = gpsstr
                        elif i== 2:
                            insertdata['speed'] = gpsstr
                            updatedict['last_vt'] = gpsstr

                    if len(updatedict):
                        updatedict['last_location_type'] = self.LOCATION_TYPE_GPS
                        updatedict['last_location_time'] = recvtimestamp
                        self._redis.hmset(imeikey, updatedict)
                        self.collect_gearinfo.update_one({'_id':ObjectId(imeiinfo['_id'])},{'$set':updatedict})

                    notifygpslist.append(insertdata)
#                    logger.debug(notifygpslist)
                    self.collect_data.insert_one(insertdata)

            elif cwdtype in ['IC46']:
                recordtimestr = recvedbuf[index:index+14]
                index+=14
                if recordtimestr.startswith('00/00/00'):
                    recordtimestamp = datetime.datetime.now()
                else:
                    recordtimestr = '20%s'%recordtimestr
#                    logger.debug(recordtimestr)
                    recordtimestamp = datetime.datetime.strptime(recordtimestr,'%Y/%m/%d %H:%M')
#                logger.debug(recordtimestamp)
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2
                tempbuf = recvedbuf[index:]
#                logger.debug(tempbuf)
                templist = tempbuf.split(',')
#                logger.debug(templist)
                for x in xrange(0,ch):
                    tmpstr = templist.pop(0)
                    if len(tmpstr):
                        state = int(tmpstr[0])
                        value = float(tmpstr[1:])
                        insertdata = dict({\
                            'deviceid':deviceid,\
                            'if_name':interfacename,\
                            'channel':x,\
                            'state':state,\
                            'value':value,\
                            'recv_time':recvtimestamp,\
                            'timestamp':timestamp,\
                            'recordtime':recordtimestamp\
                            })
                        if hasgps:
                            insertdata['gpsinfo'] = gpsinfo
                        notifymsglist.append(insertdata)
                        self.collect_data.insert_one(insertdata)
            elif cwdtype in ['RG31']:
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2

                c_type = recvedbuf[index]
                index +=1
                state = int(recvedbuf[index])
                index +=1
                valuelen,value = self.getFloatfromBuf(recvedbuf[index:])
                index+=valuelen
                insertdata = dict({\
                    'deviceid':deviceid,\
                    'if_name':interfacename,\
                    'channel':ch,\
                    'type':c_type,\
                    'state':state,\
                    'value':value,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                if hasgps:
                    insertdata['gpsinfo'] = gpsinfo
                notifymsglist.append(insertdata)
                self.collect_data.insert_one(insertdata)
            elif cwdtype in ['RG41']:
                interfacename = cwdtype[0:2]
                ch = int(recvedbuf[index:index+2])
                index+=2
                tempbuf = recvedbuf[index:]
#                logger.debug(tempbuf)
                templist = tempbuf.split(',')
#                logger.debug(templist)
                for x in xrange(0,ch):
                    tmpstr = templist.pop(0)
                    if len(tmpstr):
                        registch = int(tmpstr[0:2])
                        c_type = tmpstr[2]
                        state = int(tmpstr[3])
                        value = float(tmpstr[4:])
                        insertdata = dict({\
                            'deviceid':deviceid,\
                            'if_name':interfacename,\
                            'channel':registch,\
                            'type':c_type,\
                            'state':state,\
                            'value':value,\
                            'recv_time':recvtimestamp,\
                            'timestamp':timestamp\
                            })
                        if hasgps:
                            insertdata['gpsinfo'] = gpsinfo
                        notifymsglist.append(insertdata)
                        self.collect_data.insert_one(insertdata)
            elif cwdtype in ['CK83']:
                pass
            elif cwdtype in ['HT99']:
                lastinfodict = dict()
                lastkey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid)
                lastinfodict['last_time'] = recvtimestamp.__str__()
                self._redis.hmset(lastkey,lastinfodict)                
                # lastkey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid)
                # resultlist = self._redis.keys(lastkey)
                # if self._redis.exists(lastkey) is False:
                #     lastinfodict = dict()
                # else:
                #     lastinfodict = self._redis.hgetall(lastkey)
                #     if lastinfodict is None:
                #         lastinfodict = dict()
                # lastinfodict['last_time'] = recvtimestamp.__str__()
                # self._redis.hset(lastkey,'last_time',recvtimestamp.__str__())
            elif cwdtype in ['LB66']:
                tmpbuf = recvedbuf[index:].replace(' ','')
                tmpidx = tmpbuf.find('00:')
                if tmpidx!= -1:
                    insertdata = dict()
                    insertdata['deviceid'] = deviceid
                    insertdata['if_name'] = 'LB'
                    insertdata['timestamp'] = timestamp
                    insertdata['recv_time'] = recvtimestamp
                    tmpbuf = tmpbuf[tmpidx+3:]
                    tmplist = tmpbuf.split(',')
                    for data in tmplist:
                        lbslist = data.split(':')
                        if len(lbslist) == 2:
                            insertdata[lbslist[0]] = lbslist[1]

                    if hasgps:
                        insertdata['gpsinfo'] = gpsinfo
                    notifymsglist.append(insertdata)
                    insertresults = self.collect_data.insert_one(insertdata)
                    insertobjid = insertresults.inserted_id
                    # insertobjid = self.collect_data.insert_one(insertdata)
                    #在此通知lbs模块定位
            elif cwdtype in ['LB35']:
                tempbuf = recvedbuf[index:]
                #吃掉空格
                tempbuf = tempbuf.replace(' ','')
                templist = tempbuf.split(":")
                templen = len(templist)
                if templen >= 2:
                    tempbuf = templist[templen-1]
                    datalist = tempbuf.split(',')
                    datalen = len(datalist)
#                    logger.debug(datalist)
                    insertdata = dict()
                    if datalen >= 3:
                        updatedict = dict()
                        longitude = float(datalist[0])
                        latitude = float(datalist[1])
                        speed = float(datalist[2])
                        if longitude == 0 and latitude == 0:
                            #不正确的GPS信息
                            pass
                        else:
                            insertdata['deviceid'] = deviceid
                            insertdata['if_name'] = 'GPS'
                            insertdata['timestamp'] = timestamp
                            insertdata['recv_time'] = recvtimestamp
                            if latitude>90 or latitude<-90:
                                updatedict['last_lat'] = datalist[0]
                                updatedict['last_long'] = datalist[1]
                                updatedict['last_vt'] = datalist[2]
                                if longitude >=0:
                                    insertdata['latitude_type'] = 'N'
                                else:
                                    insertdata['latitude_type'] = 'S'
                                insertdata['latitude_value'] = str(longitude)
                                if latitude >=0:
                                    insertdata['longitude_type'] = 'E'
                                else:
                                    insertdata['longitude_type'] = 'W'
                                insertdata['longitude_value'] = str(latitude)
                                insertdata['speed'] = str(speed)
                            else:
                                updatedict['last_long'] = datalist[0]
                                updatedict['last_lat'] = datalist[1]
                                updatedict['last_vt'] = datalist[2]
                                if latitude >=0:
                                    insertdata['latitude_type'] = 'N'
                                else:
                                    insertdata['latitude_type'] = 'S'
                                insertdata['latitude_value'] = str(latitude)
                                if longitude >=0:
                                    insertdata['longitude_type'] = 'E'
                                else:
                                    insertdata['longitude_type'] = 'W'
                                insertdata['longitude_value'] = str(longitude)
                                insertdata['speed'] = str(speed)

                    if len(updatedict):
                        updatedict['last_location_type'] = self.LOCATION_TYPE_LBS
                        updatedict['last_location_time'] = recvtimestamp
                        self._redis.hmset(imeikey, updatedict)
                        self.collect_gearinfo.update_one({'_id':ObjectId(imeiinfo['_id'])},{'$set':updatedict})
                    if len(insertdata):
                        self.collect_data.insert_one(insertdata)
                        notifygpslist.append(insertdata)
            elif cwdtype in ['GM67','RS45']:
                interfacename = cwdtype[0:2]
                tmpbuf = recvedbuf[index:].replace(' ','')
                tmpidx = tmpbuf.find('00:')
                if tmpidx!= -1:
                    insertdata = dict()
                    insertdata['deviceid'] = deviceid
                    insertdata['if_name'] = interfacename
                    insertdata['timestamp'] = timestamp
                    insertdata['recv_time'] = recvtimestamp
                    tmpbuf = tmpbuf[tmpidx+3:]
                    tmplist = tmpbuf.split(',')
                    updatedict = dict()
                    for data in tmplist:
                        paramlist = data.split(':')
                        if len(paramlist) == 2:
                            if paramlist[0] in ['SIGNAL','signal']:
                                updatedict['signal_level'] = float(paramlist[1])
                                insertdata['signal_level'] = float(paramlist[1])
                            else:
                                updatedict[paramlist[0]] = paramlist[1]
                                insertdata[paramlist[0]] = paramlist[1]

                    if hasgps:
                        insertdata['gpsinfo'] = gpsinfo
                    notifymsglist.append(insertdata)
                    insertresults = self.collect_data.insert_one(insertdata)
                    insertobjid = insertresults.inserted_id
                    # insertobjid = self.collect_data.insert_one(insertdata)

#                    logger.debug(updatedict)
                    self._redis.hmset(imeikey, updatedict)
                    self.collect_gearinfo.update_one({'_id':ObjectId(imeiinfo['_id'])},{'$set':updatedict})
            elif cwdtype in ['DC03']:
                interfacename = cwdtype[0:2]
                power_supply = recvedbuf[index+2]
                updatedict = dict()
                updatedict['power_supply'] = power_supply
                insertdata = dict()
                insertdata['deviceid'] = deviceid
                insertdata['if_name'] = interfacename
                insertdata['timestamp'] = timestamp
                insertdata['recv_time'] = recvtimestamp
                insertdata['power_supply'] = power_supply
                insertdata['state'] = power_supply
                insertdata['value'] = power_supply
                insertdata['channel'] = ''
                dcdata1 = dict({\
                    'deviceid':deviceid,\
                    'if_name':'power_supply',\
                    'channel':'',\
                    'state':int(power_supply),\
                    'value':power_supply,\
                    'recv_time':recvtimestamp,\
                    'timestamp':timestamp\
                    })
                notifymsglist.append(dcdata1)

                if hasgps:
                    insertdata['gpsinfo'] = gpsinfo

                notifymsglist.append(insertdata)
                insertresults = self.collect_data.insert_one(insertdata)
                insertobjid = insertresults.inserted_id
                # insertobjid = self.collect_data.insert_one(insertdata)

#                logger.debug(updatedict)
                self._redis.hmset(imeikey, updatedict)
                self.collect_gearinfo.update_one({'_id':ObjectId(imeiinfo['_id'])},{'$set':updatedict})


            need_devicealarm = False
            alarmtype = self.GEAR_ALARM_INVALID
            if len(notifymsglist):
                #存最近的值
#                ifname = insertdata.get('if_name')
                if cwdtype not in ['GM67','RS45','CK83','LB66']:
                    lastkey = self.KEY_DEVICE_LASTDATA_FLAG % (deviceid)
                    lastinfodict = dict()
                    # resultlist = self._redis.keys(lastkey)
                    # if self._redis.exists(lastkey) is False:
                    #     lastinfodict = dict()
                    # else:
                    #     lastinfodict = self._redis.hgetall(lastkey)
                    #     if lastinfodict is None:
                    #         lastinfodict = dict()
#                        else:
#                            lastinfodict = eval(lastinfodict)
                    #在这里产生和删除相应通道的告警标志
                    for datainfo in notifymsglist:
                        ifkey = datainfo['if_name']
                        ifname = datainfo['if_name']
                        value = datainfo.get('value')
                        channelname = ""
                        if 'channel' in datainfo and datainfo['channel'] != '':
                            ifkey = "%s%d" % (ifkey, datainfo['channel'])
                            channelname = "%d" % (datainfo['channel'])
                        state = 0
                        if 'state' in datainfo and datainfo['state'] != '':
                            state = datainfo['state']
                        ifinfodict = dict()
                        for key in datainfo:
                            if key in ['timestamp','recv_time','_id','recordtime']:
                                ifinfodict[key] = datainfo[key].__str__()
                            else:
                                ifinfodict[key] = datainfo[key]
                        lastinfodict[ifkey] = ifinfodict
                        #判断通道的状态
                        if ifname not in ['DO','AS','arm_state']:
                            #在告警状态则加上alarmkey到redis
                            if state == 1:
                                addkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid, ifname, channelname)
                                #20171123修改 --  AD2的情况下写入AD2当前的值
                                if ifname == 'AD' and channelname == '2':
                                    #先看DI4是否在报警中，然后再看定时器KEY是不是不存在，并且报警的KEY是否不存在，满足条件就产生一条告警信息
                                    dialarmkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid, 'DI','4')
                                    di4timeoutkey = self.KEY_DI4_ALARM_FLAG % (deviceid)
                                    ad2alarmkey = self.KEY_AD2_CREATED_ALARM % (deviceid)
                                    if self._redis.exists(dialarmkey) is True and self._redis.exists(di4timeoutkey) is False and self._redis.exists(ad2alarmkey) is False:
                                        alarmlist = list()
                                        alarminfo = dict()
                                        alarminfo['deviceid'] = deviceid
                                        alarminfo['objectid'] = imeiinfo.get('_id')
                                        alarminfo['level'] = self.ALARM_LEVEL_INTERFACE
                                        alarminfo['value'] = value
                                        alarminfo['vdeviceid'] = ''
                                        alarminfo['appid'] = ''
                                        alarminfo['interface'] = ifname
                                        alarminfo['channel'] = int(channelname)
                                        alarminfo['ifname'] = ifkey
                                        alarminfo['current_state'] = state
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
                                            self._redis.lpush(self._config['alarmcenter']['Consumer_Queue_Name'],json.dumps(msgdict))
                                            #设置AD2已经告过警了
                                            self._redis.set(ad2alarmkey,'1')

                                    # logger.error('Set Device[%s] redis [%s]->[%r]'%(deviceid,addkey,value))
                                    self._redis.set(addkey,value)
                                elif ifname == 'DI' and channelname == '4':
                                    if self._redis.exists(addkey):
                                        #说明之前已经存在过一次报警了，就不管
                                        # logger.error('[%s]->[%s%s] already in alarm'%(deviceid,ifname,channelname))
                                    else:
                                        #如果之前不存在DI4报警，就在这里产生一个定时器
                                        di4timeoutkey = self.KEY_DI4_ALARM_FLAG % (deviceid)
                                        if self._redis.exists(di4timeoutkey) is False:
                                            self._redis.set(di4timeoutkey,'1')
                                            #从redis读取超时时间，如果没有就用默认的
                                            expiretime = self._redis.get(self.KEY_DI4AD2_ALARM_TIMEOUT)
                                            if expiretime is None:
                                                expiretime = self.DI4_ALARM_TIMEOUT
                                            else:
                                                expiretime = expiretime
                                            # logger.error('Set Device [%s] redis [%s]->expiretimeout[%r]'%(deviceid, di4timeoutkey,int(expiretime)))
                                            self._redis.expire(di4timeoutkey,int(expiretime))
                                            # logger.error('Set redis [%s]->expiretimeout[%r]'%(di4timeoutkey,expiretime))
                                    # logger.error('Set redis [%s]->[%r]'%(addkey,recvtimestamp.__str__()))
                                    self._redis.set(addkey,recvtimestamp.__str__())
                                elif ifname == 'DI' and channelname in ['0','1']:
                                    alarmlist = list()
                                    alarminfo = dict()
                                    alarminfo['deviceid'] = deviceid
                                    alarminfo['objectid'] = imeiinfo.get('_id')
                                    alarminfo['level'] = self.ALARM_LEVEL_INTERFACE
                                    alarminfo['value'] = value
                                    alarminfo['vdeviceid'] = ''
                                    alarminfo['appid'] = ''
                                    alarminfo['interface'] = ifname
                                    alarminfo['channel'] = int(channelname)
                                    alarminfo['ifname'] = ifkey
                                    alarminfo['current_state'] = state
                                    alarminfo['origin_state'] = 0
                                    alarminfo['alarm_type'] = self.ALARM_TYPE_INTERFACE_ALARM
                                    alarmlist.append(alarminfo)
                                    msgdict = dict()
                                    msgdict['action_cmd'] = 'push_alarm_list'
                                    msgdict['body'] = dict()
                                    msgdict['body']['alarmlist'] = alarmlist
                                    msgdict['sockid'] = ''
                                    msgdict['from'] = ''
                                    msgdict['version'] = '1.0'
                                    if 'alarmcenter' in self._config:
                                        self._redis.lpush(self._config['alarmcenter']['Consumer_Queue_Name'],json.dumps(msgdict))
                                else:
                                    self._redis.set(addkey, recvtimestamp.__str__())
                            else:
                            #非告警的时候就要删除key
                                addkey = self.KEY_ALARM_DEVICE_IFNAME_CHANNEL % (deviceid, ifname, channelname)
                                if self._redis.exists(addkey):
                                    self._redis.delete(addkey)
                                #如果是DI4恢复正常，就要删除之前的定期器的KEY
                                if ifname == 'DI' and channelname == '4':
                                    di4timeoutkey = self.KEY_DI4_ALARM_FLAG % (deviceid)
                                    if self._redis.exists(di4timeoutkey):
                                        # logger.error('Delete [%s] -> redis key[%s]'%(deviceid,di4timeoutkey))
                                        self._redis.delete(di4timeoutkey)
                                    ad2alarmkey = self.KEY_AD2_CREATED_ALARM % (deviceid)
                                    if self._redis.exists(ad2alarmkey):
                                        # logger.error('Delete [%s] -> redis key[%s]'%(deviceid,ad2alarmkey))
                                        self._redis.delete(ad2alarmkey)
                                # if ifname == 'AD' and channelname == '2':
                                #     ad2alarmkey = self.KEY_AD2_CREATED_ALARM % (deviceid)
                                #     if self._redis.exists(ad2alarmkey):
                                #         logger.error('Delete [%s] -> redis key[%s]'%(deviceid,ad2alarmkey))
                                #         self._redis.delete(ad2alarmkey)



                    lastinfodict['last_time'] = recvtimestamp.__str__()
                    self._redis.hmset(lastkey,lastinfodict)

                #检查用户是否在线
            if len(notifymsglist) or len(notifygpslist):
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
                            if len(notifymsglist):
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

                            if len(notifygpslist):
                                senddict = dict()
                                senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                                tmpdict = dict()
                                tmpdict['action_cmd'] = 'gps_update'
                                tmpdict['seq_id'] = 1
                                tmpdict['version'] = '1.0'
                                tmpbody = dict()
                                tmpdict['body'] = tmpbody
                                tmpbody['deviceid'] = deviceid

                                gpsdict = notifygpslist[0]
                                tmpbody.update(gpsdict)

                                if 'timestamp' in tmpbody:
                                    tmpbody['timestamp'] = tmpbody['timestamp'].__str__()
                                if 'recv_time' in tmpbody:
                                    tmpbody['recv_time'] = tmpbody['recv_time'].__str__()
                                if '_id' in tmpbody:
                                    tmpbody.pop('_id')
                                senddict['sendbuf'] = json.dumps(tmpdict)
                                #发送消息
                                if 'mqtt_publish' in self._config:
                                    self._sendMessage(self._config['mqtt_publish']['Consumer_Queue_Name'], json.dumps(senddict))
                else:
                    searchkey = self.KEY_CLIENT_ONLINE_FLAG % ('*', imeiinfo.get('member_id'))
                    resultlist = self._redis.keys(searchkey)
#                    logger.debug(resultlist)
                    if len(resultlist):
                        username = resultlist[0].split(':')[2]
                        if len(notifymsglist):
                            senddict = dict()
                            senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                            tmpdict = dict()
                            tmpdict['action_cmd'] = 'data_update'
                            tmpdict['seq_id'] = 1
                            tmpdict['version'] = '1.0'
                            tmpbody = dict()
                            tmpdict['body'] = tmpbody
                            tmpbody['deviceid'] = deviceid
    #                        logger.debug(notifymsglist)
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

                        if len(notifygpslist):
                            senddict = dict()
                            senddict['topic'] = "%s%s" % (self._mqttconfig['mqtt_publish_topic_prefix'], username)
                            tmpdict = dict()
                            tmpdict['action_cmd'] = 'gps_update'
                            tmpdict['seq_id'] = 1
                            tmpdict['version'] = '1.0'
                            tmpbody = dict()
                            tmpdict['body'] = tmpbody
                            tmpbody['deviceid'] = deviceid

                            gpsdict = notifygpslist[0]
                            tmpbody.update(gpsdict)

                            if 'timestamp' in tmpbody:
                                tmpbody['timestamp'] = tmpbody['timestamp'].__str__()
                            if 'recv_time' in tmpbody:
                                tmpbody['recv_time'] = tmpbody['recv_time'].__str__()
                            if '_id' in tmpbody:
                                tmpbody.pop('_id')
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
                    nextcmddict = eval(nextcmd)
                    sockkey = self.KEY_DEVICE_SOCKETID_FLAG % (deviceid,'*')
                    resultlist = self._redis.keys(sockkey)
                    if len(resultlist):
                        sockinfo = self._redis.get(resultlist[0])
                        msgdict = dict()
                        msgdict['action_cmd'] = 'device_ctrl'
                        msgdict['body'] = dict()
                        msgdict['body']['sendbuf'] = nextcmddict['sendbuf']
                        msgdict['body']['sockid'] = sockinfo
                        msgdict['body']['deviceid'] = deviceid
                        msgdict['body']['ctrlid'] = nextcmddict['ctrlid']
                        msgdict['sockid'] = ''
                        msgdict['from'] = ''
                        msgdict['version'] = '1.0'
                        if 'deviceproxy' in self._config:
                            self._redis.lpush(self._config['deviceproxy']['Consumer_Queue_Name'],json.dumps(msgdict))
                        self._redis.set(currentcmdkey, nextcmddict)
                        self._redis.expire(currentcmdkey,self.CTRL_CMD_TIMEOUT)
                        self._redis.expire(cachedcmdlistkey,self.CTRL_QUEUE_TIMEOUT)
                    #self._redis.rpush(cachedcmdlistkey,nextcmd)

            #在此处发送处理请求到transection模块
            msgdict = dict()
            msgdict['action_cmd'] = 'calc_device_state'
            msgdict['body'] = dict()
            msgdict['body']['deviceid'] = deviceid
            msgdict['sockid'] = ''
            msgdict['from'] = ''
            msgdict['version'] = '1.0'
            if 'transaction' in self._config:
                self._redis.lpush(self._config['transaction']['Consumer_Queue_Name'],json.dumps(msgdict))

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
    if _config is not None and 'datacenter' in _config and _config['datacenter'] is not None:
        if 'thread_count' in _config['datacenter'] and _config['datacenter']['thread_count'] is not None:
            thread_count = int(_config['datacenter']['thread_count'])

    for i in xrange(0, thread_count):
        datacenter = DataCenter(i)
        datacenter.setDaemon(True)
        datacenter.start()

    while 1:
        time.sleep(1)
