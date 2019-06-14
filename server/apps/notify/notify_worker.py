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
if '/opt/Clousky/firearm/apps/ampqapi' not in sys.path:
    sys.path.append('/opt/Clousky/firearm/apps/ampqapi')
import rabbitmq_consume as consumer
import rabbitmq_publish as publisher

if '/opt/Clousky/Ronaldo/server/apps/utils' not in sys.path:
    sys.path.append('/opt/Clousky/Ronaldo/server/apps/utils')
import getserviceinfo

if '/opt/Clousky/Ronaldo/server/apps/common' not in sys.path:
    sys.path.append('/opt/Clousky/Ronaldo/server/apps/common')
import workers

import json
import MySQLdb as mysql
import pymongo

import logging
import logging.config
import uuid
import random
import urllib

import smtplib
from email.mime.text import MIMEText
import httplib2
import hashlib
import socket

logging.config.fileConfig("/opt/Clousky/Ronaldo/server/conf/log.conf")
logr = logging.getLogger('ronaldo')


class ipServer(workers.WorkerBase):

    def __init__(self, moduleid):
        logr.debug("ipServer :running in __init__")
        workers.WorkerBase.__init__(self, moduleid)
#        self.mongoconn = pymongo.Connection(self._json_dbcfg['mongo_ip'],int(self._json_dbcfg['mongo_port']))
        self.mongoconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
#        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']))
        self.db = self.mongoconn.notify
        self.notify_log = self.db.notify_log
        self.notify_template = self.db.notify_template

    def __str__(self):
        pass
        '''

        '''

    def _proc_message(self, ch, method, properties, body):
        '''
        消息处理入口函数
        '''
        logr.debug('into _proc_message')
        t1 = time.time()
        #解body
        msgdict = dict()
        try:
            logr.debug(body)
            msgdict = json.loads(body)
        except:
            logr.error("parse body error")
            self._sendMessage(properties.reply_to, self.ERROR_RSP_UNKOWN_COMMAND, properties.correlation_id)
            return
        #检查消息必选项
        if len(msgdict) == 0:
            logr.error("body lenght is zero")
            self._sendMessage(properties.reply_to, self.ERROR_RSP_UNKOWN_COMMAND, properties.correlation_id)
            return
        if "action_cmd" not in msgdict or "seq_id" not in msgdict or "version" not in msgdict:
            logr.error("no action_cmd in body")
            self._sendMessage(properties.reply_to, self.ERROR_RSP_UNKOWN_COMMAND, properties.correlation_id)
            return
        #构建回应消息结构
        message_resp = dict({'error_code':'', 'seq_id':'', 'body':{}})
        action_resp = message_resp['body']
        #根据消息中的action逐一处理
        #判断msgdict['actions']['action']是list还是dict，如果是dict说明只有一个action，如果是list说明有多个action
        resp = self._proc_action(msgdict,message_resp)

        #发送resp消息
        jsonresp = json.dumps(message_resp)
        logr.debug(jsonresp)
        logr.debug("cmd timer = %f",time.time()-t1)

        self._sendMessage(properties.reply_to, jsonresp, properties.correlation_id)

    def _proc_action(self, action, ret):
        '''action处理入口函数'''
#        ret = dict()
#        logr.debug("_proc_action action=%s" % (action))
        action_name = action['action_cmd']
        logr.debug('action_name : %s' % (action_name))
        action_version = action['version']
        logr.debug('action_version : %s' % (action_version))
        action_seqid = action['seq_id']
        logr.debug('action_seqid : %s' % (action_seqid))
        if 'body' in action:
            action_params = action['body']
#            logr.debug('action_params : %s' % (action_params))
        else:
            action_params = None
            logr.debug('no action_params')
        #构建消息返回头结构
        ret['seq_id'] = action_seqid
        if action_name == 'send_notify':
            self._proc_action_sendnotify(ret, action_name, action_version, action_seqid, action_params)
        else:
            ret['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

        return

    def start_forever(self):
        logr.debug("running in start_forever")
        self._start_consumer()


    def _sendEmail(self, dest, content, email_from, email_subject):
        fs = open("/opt/Clousky/Ronaldo/server/conf/email.conf", "r")
        gwconfig = json.load(fs)
        fs.close()
        logr.debug(gwconfig)

        msg = MIMEText(content, _subtype=gwconfig['content_type'], _charset=gwconfig['content_charset'])
        msg['Subject'] = email_subject.encode(gwconfig['content_charset'])
        msg['From'] = email_from.encode(gwconfig['content_charset'])
        msg['To'] = dest
        try:
            #注意端口号是465，因为是SSL连接
            s = smtplib.SMTP()
            emailhost = gwconfig['gwip']
            logr.debug(emailhost)
            s.connect(emailhost)
            s.login(gwconfig['from'],gwconfig['password'])
#            print gwconfig['email']['email_from'].encode(gwconfig['email']['email_charset'])
            s.sendmail(email_from.encode(gwconfig['content_charset']), dest.split(';'), msg.as_string())
            time.sleep(self.SEND_EMAIL_INTERVAL)
            s.close()
            return '200',None
        except Exception as e:
            logr.error("%s except raised : %s " % (e.__class__, e.args))
            return '400',None      


    def _proc_action_sendnotify(self, ret, action_name, action_version, action_seqid, action_params):
        '''
        input : {    'action_cmd'  : 'gear_add', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'to'    : M
                        'content'     : M
                        'carrier'     : M
                        'notify_type'   : M
                        
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                }
        '''
        logr.debug(" into _proc_action_sendnotify action_body:%s"%action_params)
#        try:
        if 1:
            
            if ('dest' not in action_params) or  ('content' not in action_params) or  ('carrier' not in action_params) or  ('notify_type' not in action_params):
                ret['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_params['dest'] is None or action_params['content'] is None or action_params['carrier'] is None or action_params['notify_type'] is None:
                ret['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            dest = action_params['dest']
            carrier = action_params['carrier']
            notify_type = action_params['notify_type']
            content = urllib.unquote(action_params['content'].encode('utf-8')).decode('utf-8')
            if carrier == 'email':
                if 'email_from' in action_params and action_params['email_from'] is not None:
                    email_from = action_params['email_from']
                else:
                    email_from = 'noreply@keeprapid.com'

                if 'email_subject' in action_params and action_params['email_subject'] is not None:
                    email_subject = action_params['email_subject']
                else:
                    email_subject = 'From keeprapid'

                self._sendEmail(dest, content, email_from, email_subject)

            logdict = dict()
            logdict['from'] = email_from
            logdict['to'] = dest
            logdict['notify_type'] = notify_type
            logdict['carrier'] = carrier
            logdict['content'] = content
            logdict['timestamp'] = datetime.datetime.now()
            self.notify_log.insert_one(logdict)
                
            ret['error_code'] = self.ERRORCODE_OK
            return

#        except Exception as e:
#            logr.error("%s except raised : %s " % (e.__class__, e.args))
#            ret['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

        


if __name__ == "__main__":
    ''' parm1: moduleid,
    '''
    if sys.argv[1] is None:
        logr.error("Miss Parameter")
        sys.exit()
    moduleid = int(sys.argv[1])
    workers = ipServer(moduleid)
    workers.start_forever()
