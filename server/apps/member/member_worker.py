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


class MemberLogic(threading.Thread, workers.WorkerBase):

    def __init__(self, thread_index):
#        super(MemberLogic, self).__init__()
        threading.Thread.__init__(self)
        workers.WorkerBase.__init__(self)
        logger.debug("MemberLogic :running in __init__")

        fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()

        fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fileobj)
        fileobj.close()
        self.thread_index = thread_index
        self.recv_queue_name = "W:Queue:Member"
        if 'member' in _config:
            if 'Consumer_Queue_Name' in _config['member']:
                self.recv_queue_name = _config['member']['Consumer_Queue_Name']

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
        self.collect_appinfo = self.dbgear.app_info
        self.collect_vdevice = self.dbgear.vdevice_info


        self.dataconn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self._json_dbcfg['mongo_user'],self._json_dbcfg['mongo_password'],self._json_dbcfg['mongo_ip'],self._json_dbcfg['mongo_port']))
        self.dbdata = self.dataconn.datacenter
        self.collect_data = self.dbdata.data_info


    def calcpassword(self, password, verifycode):
        m0 = hashlib.md5(verifycode)
        logger.debug("m0 = %s" % m0.hexdigest())
        m1 = hashlib.md5(password + m0.hexdigest())
    #        print m1.hexdigest()
        logger.debug("m1 = %s" % m1.hexdigest())
        md5password = m1.hexdigest()
        return md5password

    def get_rand_str_no_repeat(self, n):
        allw = string.letters+string.digits
        r = random.sample(allw, n)
        return ''.join(r)

    def generator_tokenid(self, userid, timestr, verifycode):
        m0 = hashlib.md5(verifycode)
    #        print m0.hexdigest()
        m1 = hashlib.md5("%s%s%s" % (userid,timestr,m0.hexdigest()))
    #        print m1.hexdigest()
        token = m1.hexdigest()
        return token

    def run(self):
        logger.debug("Start MemberLogic pid=%s, threadindex = %s" % (os.getpid(),self.thread_index))
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
        if action_cmd  not in ['member_opc_query_gearlist']:
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

        if action_cmd == 'member_register':
            self._proc_action_member_regist(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update':
            self._proc_action_member_update(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'login':
            self._proc_action_login(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'logout':
            self._proc_action_logout(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_info':
            self._proc_action_member_info(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'change_password':
            self._proc_action_change_password(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'getback_password':
            self._proc_action_getback_password(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_add_gear':
            self._proc_action_member_add_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_query_gear':
            self._proc_action_member_query_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update_gear':
            self._proc_action_member_update_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_del_gear':
            self._proc_action_member_del_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_gear_headimg_upload':
            self._proc_action_member_gear_headimg_upload(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update_by_parent':
            self._proc_action_member_update_by_parent(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update_device_by_parent':
            self._proc_action_member_update_device_by_parent(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_opc_query_gear':
            self._proc_action_member_opc_query_gear(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_opc_query_gearlist':
            self._proc_action_member_opc_query_gearlist(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_delete':
            self._proc_action_member_delete(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_add_app':
            self._proc_action_member_add_app(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_del_app':
            self._proc_action_member_del_app(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_add_visual_device':
            self._proc_action_member_add_visual_device(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_update_visual_device':
            self._proc_action_member_update_visual_device(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'member_del_visual_device':
            self._proc_action_member_del_visual_device(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_member_regist(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_member_regist action_body:[%s]:%s"%(version,action_body))
        try:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or ('source' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return
            if action_body['source'] is None or action_body['source'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_SOURCE_INVALID
                return

            prefix = ""
            if 'prefix' in action_body and action_body['prefix'] is not None:
                prefix = urllib.unquote(action_body['prefix'].encode('utf-8')).decode('utf-8')

            platform_name = ""
            if 'platform_name' in action_body and action_body['platform_name'] is not None:
                platform_name = urllib.unquote(action_body['platform_name'].encode('utf-8')).decode('utf-8')
            logourl = ""
            if 'logourl' in action_body and action_body['logourl'] is not None:
                logourl = urllib.unquote(action_body['logourl'].encode('utf-8')).decode('utf-8')

            tid = ""
            if 'tid' in action_body and action_body['tid'] is not None:
                tid = action_body['tid']

            adminkey = self.KEY_TOKEN_NAME_ID % ('*','admin','*')
            adminresult = self._redis.keys(adminkey)
            if len(adminresult) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_ADMIN_NOT_EXIST
                return

            admininfo = self._redis.hgetall(adminresult[0])
            state = self.GEAR_STATE_NEW
#            logger.debug(admininfo)
            parent_id = ""
            role = 1
            if tid == "":
                role = 1
                parent_id = admininfo.get('_id')
                platform_name = admininfo.get('platform_name')
                logourl = admininfo.get('logourl')

            elif tid == admininfo.get('tid'):
                role = 1
                parent_id = admininfo.get('_id')
                platform_name = admininfo.get('platform_name')
                logourl = admininfo.get('logourl')
                state = self.MEMBER_STATE_EMAIL_COMFIRM
            else:
                searchkey = self.KEY_TOKEN_NAME_ID % (tid,'*','*')
                result = self._redis.keys(searchkey)
                if len(result) == 0:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                    return
                memberinfo = self._redis.hgetall(result[0])
                role = 2
                parent_id = memberinfo.get('_id')
                platform_name = memberinfo.get('platform_name')
                logourl = memberinfo.get('logourl')
                state = self.MEMBER_STATE_EMAIL_COMFIRM

            source = action_body['source']
            company_name = ""
            if 'company_name' in action_body and action_body['company_name'] is not None:
                company_name = urllib.unquote(action_body['company_name'].encode('utf-8')).decode('utf-8')

            if  tid is not None and tid!="" and role==1 and prefix!="":
                if self.collect_memberinfo.find_one({'prefix': prefix}):
                    retdict['error_code'] = self.ERRORCODE_MEMBER_PREFIX_ALREADY_EXIST
                    return
            else:
                if role>1:
                    prefix = memberinfo.get('prefix')


            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            if self.collect_memberinfo.find_one({'username': username}):
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                return
            if source == 'mobile':
                if self.collect_memberinfo.find_one({'mobile':username}):
                    retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST
                    return
            lang = 'chs'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            nickname = username
            if 'nickname' in action_body and action_body['nickname'] is not None:
                nickname = urllib.unquote(action_body['nickname'].encode('utf-8')).decode('utf-8')

            if 'email' in action_body and action_body['email'] is not None:
                email = action_body['email']
                if self.collect_memberinfo.find_one({'email':email}):
                    retdict['error_code'] = self.ERRORCODE_MEMBER_EMAIL_ALREADY_EXIST
                    return

            else:
                email = username

            if 'mobile' in action_body and action_body['mobile'] is not None:
                mobile = action_body['mobile']
            else:
                mobile = ''

            if 'enable_notify' in action_body and action_body['enable_notify'] is not None:
                enable_notify = action_body['enable_notify']
            else:
                enable_notify = '1'


            userprofile = dict()

            insertmember = dict({\
                'username':username,\
                'nickname':nickname,\
                'source':source,\
                'password':self.calcpassword(action_body['pwd'],self.MEMBER_PASSWORD_VERIFY_CODE),\
                'createtime': datetime.datetime.now(),\
                'lastlogintime':None,\
                'state': state,\
                'logourl':logourl,\
                'platform_name':platform_name,\
                'parent_id':parent_id,\
                'role':role,\
                'mobile':mobile,\
                'email':email,\
                'prefix':prefix,\
                'current_DeviceNumber':0,\
                'company_name':company_name,\
                'lang':lang,\
                'account':dict(),\
                'userprofile':userprofile,\
                'device':list()\
                })

            if role == 2:
                parentinfo = self.collect_memberinfo.find_one({'_id':ObjectId(parent_id)})
                if parentinfo is not None:
                    for key in parentinfo:
                        if key in ['prefix','platform_name','logourl']:
                            if insertmember.get(key) is None or insertmember.get(key) == "":
                                insertmember[key] = parentinfo[key]

            for key in action_body:
                if key in ['username', 'nickname', 'source', 'password', 'createtime', 'lastlogintime', 'state', 'parent_id', 'role', 'device','_id','email','mobile','tid']:
                    continue
                else:
                    if action_body[key] is not None and action_body[key] != "":
                        if key in ['address','company_name','department','job','remark']:
                            insertmember[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                        else:
                            insertmember[key] = action_body[key]

            logger.debug(insertmember)
            self.collect_memberinfo.insert_one(insertmember)

            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_login(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'login', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        'username'    : M
                        'pwd'     : M
                        'vid'     : M
                        'phone_name':O
                        'phone_os':O
                        'app_version':O
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                   body:{
                       tid:O
                       activate_flag:O
                   }
                }
        '''
        logger.debug(" into _proc_action_login action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('pwd' not in action_body) or  ('login_from' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['pwd'] is None or action_body['pwd'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return
            if action_body['username'] is None or action_body['username'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return

            if action_body['login_from'] is None or action_body['login_from'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_USERNAME_INVALID
                return

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            username_unquote = action_body['username']
            login_from = action_body['login_from'];
            #在redis中查找用户登陆信息
            tokenid = None
            memberid = ''
            addkey = ''
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username_unquote,'*')
            logger.debug("key = %s" % searchkey)
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % resultlist)
            role = 1
            parent_id = ""

            if len(resultlist):
                redis_memberinfo = self._redis.hgetall(resultlist[0])
                if redis_memberinfo is None:
                    #redis中没有用户信息，就从mongo中读取
                    member = self.collect_memberinfo.find_one({'username': username})
                    state = member['state']
                    if isinstance(state, str) or isinstance(state, unicode):
                        state = int(state)
                    if state != 1:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_IS_NOT_ACTIVE
                        return

                    if member is None:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                        return
                    #比较密码
                    if action_body['pwd'] != member['password']:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                        return
                    #产生tokenid
                    memberid = member['_id'].__str__()
                    tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                    #写入redis
                    addkey = self.KEY_TOKEN_NAME_ID % (tokenid,username_unquote, memberid)
                    self._redis.hset(addkey, 'tid', tokenid)
                    for key in member:
                        if key == '_id':
                            self._redis.hset(addkey, key, memberid)
                        elif key in ['createtime', 'lastlogintime']:
                            self._redis.hset(addkey, key, member[key].__str__())
                        elif key == 'tid':
                            continue
                        else:
                            self._redis.hset(addkey, key, member[key])
                    #删除无用的key
                    self._redis.delete(resultlist[0])
                    role = member.get('role')
                    parent_id = member.get('parent_id')

                else:
                    memberid = redis_memberinfo['_id']
                    tokenid = redis_memberinfo['tid']
                    password = redis_memberinfo['password']
                    addkey = resultlist[0]
                    #比较密码
                    if action_body['pwd'] != password:
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                        return
                    role = redis_memberinfo.get('role')
                    parent_id = redis_memberinfo.get('parent_id')

            else:
                member = self.collect_memberinfo.find_one({'username': username})
                if member is None:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                    return
                #比较密码
                if action_body['pwd'] != member['password']:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                    return
                state = member['state']
                logger.debug(member['state'].__class__)
                if isinstance(state, str) or isinstance(state, unicode):
                    state = int(state)
                if state != 1:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_IS_NOT_ACTIVE
                    return
                #产生tokenid
                memberid = member['_id'].__str__()
                tokenid = self.generator_tokenid(memberid, str(datetime.datetime.now()), self.MEMBER_PASSWORD_VERIFY_CODE)
                #写入redis
                addkey = self.KEY_TOKEN_NAME_ID % (tokenid,username_unquote, memberid)
                self._redis.hset(addkey, 'tid', tokenid)
                for key in member:
                    if key == '_id':
                        self._redis.hset(addkey, key, memberid)
                    elif key in ['createtime', 'lastlogintime']:
                        self._redis.hset(addkey, key, member[key].__str__())
                    elif key == 'tid':
                        continue
                    else:
                        self._redis.hset(addkey, key, member[key])

                role = member.get('role')
                parent_id = member.get('parent_id')

            #更新上次登陆时间
            t = datetime.datetime.now()
            self._redis.hset(addkey, 'lastlogintime', t.__str__())
            self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':{'lastlogintime':t}})



            #添加登陆记录
            insertlog = dict({\
                'mid':memberid,\
                'tid':tokenid,\
                'login_from':login_from,\
                'timestamp': datetime.datetime.now()\
                })
            logger.debug(insertlog)
            self.collect_memberlog.insert_one(insertlog)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['tid'] = tokenid
            retbody['role'] = role
            retbody['parent_id'] = parent_id
            retbody['memberid'] = memberid

            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_logout(self, version, action_body, retdict, retbody):
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
        logger.debug(" into _proc_action_logout action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            tid = action_body['tid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = '200'
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_add_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_add_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('template_id' not in action_body) or ('count' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['template_id'] is None or action_body['count'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return
            if 'group_id' in action_body and action_body['group_id'] is not None:
                group_id = action_body['group_id']
            else:
                group_id = ''

            special_application = self.SPECIAL_APPLICATION_NORMAL
            if 'special_application' in action_body:
                special_application = action_body['special_application']
                if isinstance(special_application, int):
                    special_application = int(special_application)



            template_id = action_body['template_id']
            tid = action_body['tid']
            count_str = action_body['count']
            count = int(count_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            objectid_memberid = ObjectId(tni['_id'])
            memberid = tni['_id']

            memberinfo = self.collect_memberinfo.find_one({'_id':objectid_memberid})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            gearinfolist = self.collect_gearinfo.find({'prefix':memberinfo['prefix']})
            maxdevicenumber = 0
            for gearinfo in gearinfolist:
                deviceid = str(gearinfo['deviceid'])
                devicenumber = int(deviceid[3:])
                if devicenumber>maxdevicenumber:
                    maxdevicenumber = devicenumber

#            current_DeviceNumber = memberinfo.get('current_DeviceNumber')
#            if current_DeviceNumber is None:
#                current_DeviceNumber = 0
            current_DeviceNumber = maxdevicenumber
            logger.debug("current_DeviceNumber=%d" % (current_DeviceNumber))
            finishDeviceNumber = current_DeviceNumber+count
            self.collect_memberinfo.update_one({'_id':objectid_memberid},{'$set':{'current_DeviceNumber':finishDeviceNumber}})

            devicelist = eval(tni.get('device'))
            prefix = tni.get('prefix')
            lastDid = current_DeviceNumber
            insertlist = list()
            insertloglist = list()
            i = 0
            while 1:
#                imei = '%s%s' % (prefix,self.get_rand_str_no_repeat(5))
                lastDid = lastDid+1
                imei = '%s%.5d' %(prefix, lastDid)
#                logger.debug("deviceid=%s" % (imei))
                memberlist = list()
                memberlist.append(memberid)
                
                if imei not in devicelist:
                    insertdevice = dict({\
                        'deviceid':imei,\
                        'template_id':template_id,\
                        'name':"",\
                        'group_id':group_id,\
                        'sub_name':'',\
                        'createtime': datetime.datetime.now(),\
                        'member_id': memberlist,\
                        'state': 0,\
                        'mobile':'',\
                        'battery_level':0,\
                        'd_timeout':15*60,\
                        'power_supply':'0',\
                        'alarm_state' :'0',\
                        'temperature' : 0,\
                        'signal_level':0,\
                        'last_location_type':'',\
                        'last_long':0,\
                        'last_lat':0,\
                        'last_vt':0,\
                        'last_at':0,\
                        'last_angel':0,\
                        'last_location_time': datetime.datetime.now(),\
                        'enable_alarm':0,\
                        'alarm_lat':0,\
                        'alarm_long':0,\
                        'ownerid':memberid,\
                        'prefix':prefix,\
                        'special_application':special_application,\
                        'alarm_radius':0\
                        })
                    for key in action_body:
                        if key in ['tid','count','template_id','group_id']:
                            continue
                        insertdevice[key]=action_body[key]
                    
#                    logger.debug(insertdevice)
                    insertlist.append(insertdevice)
#                    devicemongoid = self.collect_gearinfo.insert(insertdevice)
#                    addkey = self.KEY_IMEI_ID % (imei, devicemongoid.__str__())
#                    insertdevice['_id'] = devicemongoid.__str__()
#                    self._redis.hmset(addkey,insertdevice)
#                    addkey = self.KEY_MEMBER_IMEI % (memberid, imei)
#                    self._redis.hmset(addkey,{'memberid':memberid, 'deviceid':imei})

#                    insertlog = dict({\
#                        'device_id': devicemongoid.__str__(),\
#                        'member_id': memberid,\
#                        'action': 'add',\
#                        'timestamp': datetime.datetime.now()})
#                    insertloglist.append(insertlog)
#                    self.collect_gearlog.insert(insertlog)
                    devicelist.append(imei)
                    i = i+1
                    if i>= count:
                        logger.debug("i=%d, close" % (i))
                        break
            insertresults = self.collect_gearinfo.insert_many(insertlist)
#            insertdeviceidlist = self.collect_gearinfo.insert_many(insertlist)
            insertdeviceidlist = insertresults.inserted_ids
            for i in xrange(0,len(insertdeviceidlist)):
                devicemongoid = insertdeviceidlist[i].__str__()
                deviceinfo = insertlist[i]
                addkey = self.KEY_IMEI_ID % (deviceinfo['deviceid'], devicemongoid)
                deviceinfo['_id'] = devicemongoid
                self._redis.hmset(addkey,deviceinfo)
                insertlog = dict({\
                    'device_id': deviceinfo['deviceid'],\
                    'member_id': memberid,\
                    'action': 'add',\
                    'prefix': prefix,\
                    'objectid':devicemongoid,\
                    'tablename':'device_info',\
                    'timestamp': datetime.datetime.now()})
                insertloglist.append(insertlog)

            self._redis.hset(tnikey, 'device', devicelist)
            self.collect_memberinfo.update_one({'_id':objectid_memberid},{'$set':{'device':devicelist}})
            self.collect_gearlog.insert_many(insertloglist)

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_info(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_info action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            tid = action_body['tid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            for key in tni:
                if key in ['username', 'nickname']:
                    retbody[key] = urllib.quote(tni[key])
                elif key in ['_id', 'password', 'createtime','device']:
                    continue
                elif key in ['account', 'userprofile']:
                    retbody[key] = eval(tni[key])
                else:
                    retbody[key] = tni[key]


            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_query_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei O  
                        vid
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_query_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

            imei_str = None
            imei = None
            if 'deviceid' in action_body and action_body['deviceid'] is not None:
                imei_str = action_body['deviceid']
                imei = int(imei_str)

            retbody['gearinfo'] = list()

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_OK
                retbody['gearinfo'] = list()
                return
            deviceinfo = eval(tni['device'])

            if imei_str is not None and imei is not None:
                #只查单条信息
                if imei_str not in deviceinfo:
                    retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                    retbody['gearinfo'] = list()
                    return
                else:
                    #先找内存里面的
                    searchkey = self.KEY_IMEI_ID % (imei_str, '*')
                    resultlist = self._redis.keys(searchkey)
                    if len(resultlist):
                        imeikey = resultlist[0]
                        imeiinfo = self._redis.hgetall(imeikey)
                        imeidict = dict()
                        for key in imeiinfo:
                            if key in ['member_id','maxfollowcount', '_id']:
                                continue
                            elif key in ['nickname','img_url']:
                                imeidict[key] = urllib.quote(imeiinfo[key])
                            else:
                                imeidict[key] = imeiinfo[key]
                        retbody['gearinfo'].append(imeidict)
                        retdict['error_code'] = self.ERRORCODE_OK
                        return
                    else:
                        #否则找数据库的
                        imeiinfo = self.collect_gearinfo.find_one({'deviceid':imei})
                        if imeiinfo:                           
                            imeidict = dict()
                            for key in imeiinfo:
                                if key in ['member_id','maxfollowcount','_id']:
                                    continue
                                elif key in ['createtime', 'activetime']:
                                    imeidict[key] = imeiinfo[key].__str__()
                                elif key in ['nickname','img_url']:
                                    imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                                else:
                                    imeidict[key] = imeiinfo[key]
                            retbody['gearinfo'].append(imeidict)
                            retdict['error_code'] = self.ERRORCODE_OK
                            return
                        else:
                            retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                            return
            else:
                for imei_str in deviceinfo:
                    #先找内存里面的
                    searchkey = self.KEY_IMEI_ID % (imei_str, '*')
                    resultlist = self._redis.keys(searchkey)
                    if len(resultlist):
                        imeikey = resultlist[0]
                        imeiinfo = self._redis.hgetall(imeikey)
                        imeidict = dict()
                        for key in imeiinfo:
                            if key in ['member_id','maxfollowcount']:
                                continue
                            elif key in ['nickname','img_url']:
                                imeidict[key] = urllib.quote(imeiinfo[key])
                            else:
                                imeidict[key] = imeiinfo[key]
                        retbody['gearinfo'].append(imeidict)
                    else:
                        #否则找数据库的
                        imeiinfo = self.collect_gearinfo.find_one({'deviceid':imei})
                        if imeiinfo:                           
                            imeidict = dict()
                            for key in imeiinfo:
                                if key in ['member_id','maxfollowcount','_id']:
                                    continue
                                elif key in ['createtime', 'activetime']:
                                    imeidict[key] = imeiinfo[key].__str__()
                                elif key in ['nickname','img_url']:
                                    imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                                else:
                                    imeidict[key] = imeiinfo[key]
                            retbody['gearinfo'].append(imeidict)

                retdict['error_code'] = self.ERRORCODE_OK
                return


            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_update(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                        email   O   
                        mobile  O   手机
                        nickname    O   nickname 用qutoe编码
                        ios_device_token    O   
                        enable_notify   O   0-no 1-yes
                        birthday        
                        lang
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            tid = action_body['tid']

            updatedict = dict()
            userprofiledict = dict()
            for key in action_body:
                if key in ['email','mobile','birthday','state']:
                    updatedict[key] = action_body[key]
                elif key == 'nickname':
                    updatedict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                elif key in ['ios_device_token', 'enable_notify', 'lang']:
                    userprofiledict[key] = action_body[key]
                else:
                    updatedict[key] = action_body[key]

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            member_id = ObjectId(tni['_id'])
            for key in updatedict:
                self._redis.hset(tnikey, key, updatedict[key])

            if 'userprofile' in tni:
                old_userprofiledict = eval(tni['userprofile'])
            else:
                old_userprofiledict = dict()

            for key in userprofiledict:
                old_userprofiledict[key] = userprofiledict[key]

            self._redis.hset(tnikey, 'userprofile', old_userprofiledict)

            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':updatedict})
            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'userprofile':old_userprofiledict}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_update_by_parent(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                        email   O   
                        mobile  O   手机
                        nickname    O   nickname 用qutoe编码
                        ios_device_token    O   
                        enable_notify   O   0-no 1-yes
                        birthday        
                        lang
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update_by_parent action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('member_id' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['member_id'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            tid = action_body['tid']
            member_id = action_body['member_id']


            updatedict = dict()
            userprofiledict = dict()
            for key in action_body:
                if key in ['email','mobile','birthday']:
                    updatedict[key] = action_body[key]
                elif key == 'state':
                    updatedict[key] = int(action_body[key])
                elif key == 'nickname':
                    updatedict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                elif key in ['ios_device_token', 'enable_notify', 'lang']:
                    userprofiledict[key] = action_body[key]
                elif key in ['tid','vid','member_id']:
                    continue
                else:
                    updatedict[key] = action_body[key]

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            parent_id = tni['_id']

            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(member_id)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            if memberinfo.get('parent_id') != parent_id:
                retdict['error_code'] = self.ERRORCODE_MEMBER_IS_NOT_YOU_CHILD
                return

            if 'prefix' in action_body and action_body['prefix'] is not None and memberinfo['role'] != 2:
                prefix = action_body['prefix']
                existprefix = self.collect_memberinfo.find({'prefix':prefix})
                for exminfo in existprefix:
                    if exminfo['_id'].__str__() != memberinfo['_id'].__str__():
                        retdict['error_code'] = self.ERRORCODE_MEMBER_PREFIX_ALREADY_EXIST
                        return


            searchkey = self.KEY_TOKEN_NAME_ID % ('*','*',member_id)
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) != 0:
                memberkey = resultlist[0]
#                for key in updatedict:
#                    self._redis.hset(memberkey, key, updatedict[key])
                self._redis.hmset(memberkey,updatedict[key])

                if 'userprofile' in tni:
                    old_userprofiledict = eval(tni['userprofile'])
                else:
                    old_userprofiledict = dict()

                for key in userprofiledict:
                    old_userprofiledict[key] = userprofiledict[key]

                self._redis.hset(memberkey, 'userprofile', old_userprofiledict)
            else:
                old_userprofiledict = memberinfo.get('userprofile')
                for key in userprofiledict:
                    old_userprofiledict[key] = userprofiledict[key]

            self.collect_memberinfo.update_one({'_id':ObjectId(member_id)}, {'$set':updatedict})
            self.collect_memberinfo.update_one({'_id':ObjectId(member_id)}, {'$set':{'userprofile':old_userprofiledict}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_change_password(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'change_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                    tid M   tokenid
                    vid M   
                    old_password    M   用户密码（verifycode = “abcdef”, 
                                        先用m0 = md5(verifycode),
                                        再m1 = md5(password+m0)

                    new_password    M   用户密码（verifycode = “abcdef”, 
                                        先用m0 = md5(verifycode),
                                        再m1 = md5(password+m0)

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_change_password action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('old_password' not in action_body) or  ('new_password' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['old_password'] is None or action_body['new_password'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            old_password = action_body['old_password']
            new_password = action_body['new_password']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            member_id = ObjectId(tni['_id'])
            
            if 'password' not in tni or tni['password'] != old_password:
                retdict['error_code'] = self.ERRORCODE_MEMBER_PASSWORD_INVALID
                return

            self._redis.hset(tnikey, 'password', new_password)    
            self.collect_memberinfo.update_one({'_id':member_id}, {'$set':{'password':new_password}})
            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL





    def _proc_action_getback_password(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'getback_password', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        username    M   用户名
                        vid M   
                        lang    O   当前版本的语言
                                    eng
                                    chs
                                    cht
                                    …

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_getbackpassword action_body")
        try:
#        if 1:
            
            if ('username' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['username'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            lang = 'eng'
            if 'lang' in action_body and action_body['lang'] is not None:
                lang = action_body['lang']

            username = urllib.unquote(action_body['username'].encode('utf-8')).decode('utf-8')
            vid = action_body['vid']
            memberinfo = self.collect_memberinfo.find_one({'username':username})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            if 'email' in memberinfo and memberinfo['email'] is not None and memberinfo['email'] != '':
                email = memberinfo['email']
            else:
                email = memberinfo['username']

            newpassword = "%d" % random.randint(100000,999999)
            md5password = self.calcpassword(newpassword, self.MEMBER_PASSWORD_VERIFY_CODE)
            logger.debug("newpassword = %s, md5password = %s" % (newpassword,md5password))
            #找对应的语言模板
            fileobj = open("/opt/Keeprapid/Areca/server/conf/notifytempelate.conf","r")
            templateinfo = json.load(fileobj)
            fileobj.close()

            getbackpassword_t = templateinfo[self.NOTIFY_TYPE_GETBACKPASSWORD]
            if lang not in getbackpassword_t:
                templateinfo = getbackpassword_t['eng']
            else:
                templateinfo = getbackpassword_t[lang]

            sendcontent = templateinfo['content'] % (username, newpassword)
            body = dict()
            body['dest'] = email
            body['content'] = urllib.quote(sendcontent.encode('utf-8'))
            body['carrier'] = 'email'
            body['notify_type'] = self.NOTIFY_TYPE_GETBACKPASSWORD
            body['email_from'] = urllib.quote(templateinfo['email_from'].encode('utf-8'))
            body['email_subject'] = urllib.quote(templateinfo['email_subject'].encode('utf-8'))
            action = dict()
            action['body'] = body
            action['version'] = '1.0'
            action['action_cmd'] = 'send_email_notify'
            action['seq_id'] = '%d' % random.randint(0,10000)
            action['from'] = ''

            self._sendMessage(self._config['notify']['Consumer_Queue_Name'], json.dumps(action))

            #修改密码
            self.collect_memberinfo.update_one({'username':username},{'$set':{'password':md5password}})
            #删除redis的cache数据
            searchkey = self.KEY_TOKEN_NAME_ID % ('*',username, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = self.ERRORCODE_OK
            return


        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_update_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id
                        mobile  O   设备手机号
                        name    O   用户名
                        gender  O   用户性别
                        height  O   cm
                        weight  O   kg
                        bloodtype   O   
                        stride  O   
                        birth   O   ‘2014-09-23’
                        relationship    O   
                        alarm_enable    O   
                        alarm_center_lat    O   警戒围栏中心纬度
                        alarm_center_long   O   警戒围栏中心经度
                        alarm_center_radius O   警戒围栏半径（米）
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('deviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['deviceid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']
            deviceid = action_body['deviceid']

            updategeardict = dict()
            for key in action_body:
                if key in ['tid','vid','deviceid']:
                    continue
                elif key in ['last_vt','last_long','last_lat','last_angel','battery_level','temperature']:
                    value = action_body[key]
                    if isinstance(value, str):
                        value = float(value)
                    updategeardict[key] = value
                elif key in ['d_timeout','state','special_application']:
                    value = action_body[key]
                    if isinstance(value, str):
                        value = int(value)
                    updategeardict[key] = value
                else:
                    updategeardict[key] = action_body[key]

            searchkey = self.KEY_IMEI_ID % (deviceid, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            devicekey = resultlist[0]
            deviceinfo = self._redis.hgetall(devicekey)


            logger.debug(updategeardict)
            if len(updategeardict):
                self._redis.hmset(devicekey, updategeardict)
                self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':updategeardict})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_del_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_del_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_del_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('deviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['deviceid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            tid = action_body['tid']
            deviceid = action_body['deviceid']
#            imei = int(imei_str)

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']

            searchkey = self.KEY_IMEI_ID % (deviceid, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self.collect_gearinfo.find_one({'deviceid':deviceid})

            #检查member是否有添加过imei
            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            #检查gear是否被member关注
            if 'member_id' not in imeiinfo or imeiinfo['member_id'] is None or imeiinfo['member_id'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                return

            #检查gear的owener是不是member
            ownerid = imeiinfo.get('ownerid')
            if ownerid is None or ownerid != memberid:
                #不是onwer 就只删除当前用户和设备的关联
                logger.debug("not owner, delete relationship")
                device = eval(tni['device'])
                if deviceid in device:
                    device.remove(deviceid)
                follow = imeiinfo['member_id']
                if isinstance(follow, list) is False:
                    tmp = follow
                    follow = list()
                    follow.append(tmp)
                if memberid in follow:
                    follow.remove(memberid)
                self._redis.hset(tnikey , 'device', device)
                self._redis.hset(imeikey, 'member_id', follow)

                self.collect_memberinfo.update_one({'_id':object_member_id},{'$set':{'device':device}})
                self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':{'member_id':follow}})
            else:
                #是owner的话就删除一切数据
                logger.debug("is owner, delete all relationship")
                follow = imeiinfo['member_id']
                #删除所有关注这个设备的用户device
                for memberid in follow:
                    memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
                    if memberinfo is None:
                        continue
                    devicelist = memberinfo.get('device')
                    if deviceid in devicelist:
                        devicelist.remove(deviceid)
                        searchkey = self.KEY_TOKEN_NAME_ID % ('*','*',memberid)
                        resultlist = self._redis.keys(searchkey)
                        if len(resultlist):
                            memberkey = resultlist[0]
                            self._redis.hset(memberkey,'device', devicelist)
                            self.collect_memberinfo.update_one({'_id':ObjectId(memberid)},{'$set':{'device':devicelist}})
                #删除设备
                self.collect_gearinfo.remove({'deviceid':deviceid})
                #删除设备数据
                self.collect_data.remove({'deviceid':deviceid})
                #删除内存key
                deletekey = '*%s*' % (deviceid)
                resultlist = self._redis.keys(deletekey)
                if len(resultlist):
                    self.redisdelete(resultlist)
                #增加log
                logdict = dict()
                logdict['device_id'] = imeiinfo['deviceid']
                logdict['objectid'] = deviceid
                logdict['tablename'] = 'device_info'
                logdict['member_id'] = ownerid
                logdict['action'] = 'del'
                logdict['timestamp'] = datetime.datetime.now()
                self.collect_gearlog.insert_one(logdict)


            retdict['error_code'] = self.ERRORCODE_OK
            return

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL
            return

    def _proc_action_member_gear_headimg_upload(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_del_gear', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   用户id
                        vid M   厂商id

                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_gear_headimg_upload action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or  ('deviceid' not in action_body) or  ('img' not in action_body) or  ('format' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['deviceid'] is None or action_body['format'] is None or action_body['img'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            tid = action_body['tid']
            imei_str = action_body['deviceid']
            imei = int(imei_str)

            format = action_body['format']
            img = action_body['img']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            object_member_id = ObjectId(tni['_id'])
            memberid = tni['_id']

            searchkey = self.KEY_IMEI_ID % (imei_str, '*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_IMEI_CACHE_ERROR
                return
            imeikey = resultlist[0]
            imeiinfo = self._redis.hgetall(imeikey)

            #检查member是否有添加过imei
            if 'device' not in tni or tni['device'] is None or tni['device'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_FOLLOW_GEAR
                return
            #检查gear是否被member关注
            if 'member_id' not in imeiinfo or imeiinfo['member_id'] is None or imeiinfo['member_id'] == '':
                retdict['error_code'] = self.ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW
                return

            #找到user，处理上传的头像
            fileobj = open("/opt/Keeprapid/Areca/server/conf/upload.conf",'r')
            uploadconfig = json.load(fileobj)
            fileobj.close()

            filedir = "%s%s" % (uploadconfig['FILEDIR_IMG_HEAD'], vid)
            filename = "%s.%s" % (imei_str, format)
            imgurl = "%s/%s/%s" % (uploadconfig['imageurl'],vid,filename)
            logger.debug(filedir)
            logger.debug(filename)
            logger.debug(imgurl)

            if os.path.isdir(filedir):
                pass
            else:
                os.mkdir(filedir)

            filename = "%s/%s" % (filedir, filename)
            fs = open(filename, 'wb')
            fs.write(base64.b64decode(img))
            fs.flush()
            fs.close()

            retbody['img_url'] = imgurl

            self._redis.hset(imeikey, 'img_url', imgurl)
            self.collect_gearinfo.update_one({'deviceid':imei},{'$set':{'img_url':imgurl}})

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_update_device_by_parent(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'member_update', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        vid M   厂商id
                        email   O   
                        mobile  O   手机
                        nickname    O   nickname 用qutoe编码
                        ios_device_token    O   
                        enable_notify   O   0-no 1-yes
                        birthday        
                        lang
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update_device_by_parent action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body) or ('member_id' not in action_body) or ('device_list' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None or action_body['member_id'] is None or action_body['device_list'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            tid = action_body['tid']
            member_id = action_body['member_id']
            device_list = action_body['device_list']


            updatedict = dict()
            userprofiledict = dict()
            for key in action_body:
                if key in ['email','mobile','birthday','state']:
                    updatedict[key] = action_body[key]
                elif key == 'nickname':
                    updatedict[key] = urllib.unquote(action_body[key].encode('utf-8')).decode('utf-8')
                elif key in ['ios_device_token', 'enable_notify', 'lang']:
                    userprofiledict[key] = action_body[key]
                elif key in ['tid','vid','member_id']:
                    continue
                else:
                    updatedict[key] = action_body[key]

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return
            parent_id = tni.get('_id')

            parent_devicelist = list()
            if tni['device'] != None:
                parent_devicelist = eval(tni['device'])


            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(member_id)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            if memberinfo.get('parent_id') != parent_id:
                retdict['error_code'] = self.ERRORCODE_MEMBER_IS_NOT_YOU_CHILD
                return

            old_devicelist = list()
            if memberinfo.get('device') != None:
                old_devicelist = memberinfo.get('device')


            addlist = list(set(device_list).difference(set(old_devicelist)))
            dellist = list(set(old_devicelist).difference(set(device_list)))

            logger.debug(addlist)
            logger.debug(dellist)

            for did in addlist:
                if did == "":
                    continue

                deviceinfo  = self.collect_gearinfo.find_one({'deviceid':did})
                if deviceinfo is not None:
#                    logger.debug(deviceinfo)
                    dmidlist = deviceinfo.get('member_id')
#                    logger.debug(dmidlist)
                    if isinstance(dmidlist,list) is False:
                        tmpid = dmidlist
                        dmidlist = list()
                        dmidlist.append(tmpid)
#                    logger.debug(dmidlist)
                    dmidlist.append(member_id)
                    dmidlist = list(set(dmidlist))
                    self.collect_gearinfo.update_one({'deviceid':did},{'$set':{'member_id':dmidlist}})

                    devicekey = self.KEY_IMEI_ID%(did, '*')
                    resultlist = self._redis.keys(devicekey)
                    if len(resultlist) >0:
                        self._redis.hset(resultlist[0],'member_id',dmidlist)

            for did in dellist:
                if did == "":
                    continue
                    
                deviceinfo  = self.collect_gearinfo.find_one({'deviceid':did})
                if deviceinfo is not None:
                    dmidlist = deviceinfo.get('member_id')
                    if isinstance(dmidlist,list) is False:
                        tmpid = dmidlist
                        dmidlist = list()
                        dmidlist.append(tmpid)

                    dmidlist = list(set(dmidlist))
                    if member_id in dmidlist:
                        dmidlist.remove(member_id)
                    self.collect_gearinfo.update_one({'deviceid':did},{'$set':{'member_id':dmidlist}})
                    devicekey = self.KEY_IMEI_ID%(did, '*')
                    resultlist = self._redis.keys(devicekey)
                    if len(resultlist) >0:
                        self._redis.hset(resultlist[0],'member_id',dmidlist)
                        

            old_devicelist = list(set(device_list))
            logger.debug(old_devicelist)
            self.collect_memberinfo.update_one({'_id':ObjectId(member_id)},{'$set':{'device':old_devicelist}})
            searchkey = self.KEY_TOKEN_NAME_ID % ('*','*',member_id)
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) != 0:
                self._redis.hset(resultlist[0], 'device', old_devicelist)

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_opc_query_gearlist(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei O  
                        vid
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_opc_query_gearlist action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

            imei_str = None
            imei = None
            if 'deviceid' in action_body and action_body['deviceid'] is not None:
                imei_str = action_body['deviceid']
                imei = int(imei_str)

#            retbody['gearinfo'] = list()
#            retbody['templateinfo'] = templateinfo

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_OK
                retbody['gearinfo'] = list()
                return
            deviceinfo = eval(tni['device'])
            gearinfolist = list()
            templateinfo = dict()
            templatelist = list()
            retbody['gearinfo'] = gearinfolist
            retbody['templateinfo'] = templateinfo
            for imei_str in deviceinfo:
                imeiinfo = self.collect_gearinfo.find_one({'deviceid':imei_str})
                if imeiinfo:
                    imeidict = dict()
                    for key in imeiinfo:
                        if key in ['member_id','maxfollowcount','_id']:
                            continue
                        elif key in ['createtime', 'activetime','last_location_time','uid']:
                            imeidict[key] = imeiinfo[key].__str__()
                        elif key in ['nickname','img_url']:
                            imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                        elif key in ['template_id']:
                            imeidict[key] = imeiinfo[key]
                            templatelist.append(imeiinfo[key])
                        else:
                            imeidict[key] = imeiinfo[key]
                    gearinfolist.append(imeidict)

            templateset = set(templatelist)
            for tempid in templateset:
                tempinfo = self.collect_template.find_one({'_id':ObjectId(tempid)})
#                logger.debug(tempinfo)
                if tempinfo is not None:
                    ifdict = tempinfo.get('interfacedict')
                    if isinstance(ifdict, list) is False:
                        ifdict = eval(ifdict)
#                    logger.debug(ifdict)
                    for obj in ifdict:
                        name = obj.get('name')
                        if name in ['DI','DO']:
                            obj['datatype'] = 'bool'
                        elif name in ['AI','AD','ET','TM','HU','RG']:
                            obj['datatype'] = 'float'
                        elif name in ['arm_state','power_supply','signal_level']:
                            obj['datatype'] = 'int'
                        else:
                            obj['datatype'] = 'string'

                        if 'desc' in obj:
                            obj['desc'] = urllib.quote(obj['desc'])
                        if 'alias' in obj:
                            obj['alias'] = urllib.quote(obj['alias'])
                    templateinfo[tempid] = ifdict

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_opc_query_gear(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei O  
                        vid
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_opc_query_gear action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['tid']
            tid = action_body['tid']

            imei_str = None
#            imei = None
            if 'deviceid' in action_body and action_body['deviceid'] is not None:
                imei_str = action_body['deviceid']
#                imei = int(imei_str)

            retbody['gearinfo'] = list()

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            if 'device' not in tni or tni['device'] is None:
                retdict['error_code'] = self.ERRORCODE_OK
                retbody['gearinfo'] = list()
                return
            deviceinfo = eval(tni['device'])
            gearinfolist = list()
            templateinfo = dict()
            templatelist = list()
            retbody['gearinfo'] = gearinfolist
            retbody['templateinfo'] = templateinfo
            if imei_str is not None:
                if imei_str not in deviceinfo:
                    retdict['error_code'] = self.ERRORCODE_OK
                    return
                imeiinfo = self.collect_gearinfo.find_one({'deviceid':imei_str})
                if imeiinfo:                           
                    imeidict = dict()
                    for key in imeiinfo:
                        if key in ['member_id','maxfollowcount','_id']:
                            continue
                        elif key in ['createtime', 'activetime','last_location_time','uid']:
                            imeidict[key] = imeiinfo[key].__str__()
                        elif key in ['nickname','img_url']:
                            imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                        elif key in ['template_id']:
                            imeidict[key] = imeiinfo[key]
                            templatelist.append(imeiinfo[key])
                        else:
                            imeidict[key] = imeiinfo[key]
                    #取当前数据：
                    searchkey = self.KEY_DEVICE_LASTDATA_FLAG % (imei_str)
                    lastdata = self._redis.hgetall(searchkey)
                    lastdatadict = dict()
                    if lastdata is not None:
                        for key in lastdata:
                            if key == 'last_time':
                                lastdatadict[key] = lastdata[key]
                                continue
                            data = lastdata[key]
#                            logger.debug(data)
#                            logger.debug(data.__class__)
                            if isinstance(data, dict) is False:
                                data = eval(data)
#                            logger.debug(data)
#                            logger.debug(data.__class__)
                            datainfo = dict()
                            datainfo['value'] = data.get('value')
                            datainfo['timestamp'] = data.get('timestamp')
                            datainfo['state'] = data.get('state')
                            if 'type' in data:
                                datainfo['type'] = data['type']
                            lastdatadict[key] = datainfo
                    imeidict['lastdata'] = lastdatadict
                    gearinfolist.append(imeidict)
            else:
                for imei_str in deviceinfo:
                    imeiinfo = self.collect_gearinfo.find_one({'deviceid':imei_str})
                    if imeiinfo:                           
                        imeidict = dict()
                        for key in imeiinfo:
                            if key in ['member_id','maxfollowcount','_id']:
                                continue
                            elif key in ['createtime', 'activetime','last_location_time','uid']:
                                imeidict[key] = imeiinfo[key].__str__()
                            elif key in ['nickname','img_url']:
                                imeidict[key] = urllib.quote(imeiinfo[key].encode('utf-8'))
                            elif key in ['template_id']:
                                imeidict[key] = imeiinfo[key]
                                templatelist.append(imeiinfo[key])
                            else:
                                imeidict[key] = imeiinfo[key]
                        #取当前数据：
                        searchkey = self.KEY_DEVICE_LASTDATA_FLAG % (imei_str)
                        lastdata = self._redis.hgetall(searchkey)
                        lastdatadict = dict()
                        if lastdata is not None:
                            for key in lastdata:
                                if key == 'last_time':
                                    lastdatadict[key] = lastdata[key]
                                    continue
                                data = lastdata[key]
    #                            logger.debug(data)
    #                            logger.debug(data.__class__)
                                if isinstance(data, dict) is False:
                                    data = eval(data)
    #                            logger.debug(data)
    #                            logger.debug(data.__class__)
                                datainfo = dict()
                                datainfo['value'] = data.get('value')
                                datainfo['timestamp'] = data.get('timestamp')
                                datainfo['state'] = data.get('state')
                                if 'type' in data:
                                    datainfo['type'] = data['type']
                                lastdatadict[key] = datainfo
                        imeidict['lastdata'] = lastdatadict
                        gearinfolist.append(imeidict)

                templateset = set(templatelist)
                for tempid in templateset:
                    tempinfo = self.collect_template.find_one({'_id':ObjectId(tempid)})
                    logger.debug(tempinfo)
                    if tempinfo is not None:
                        ifdict = tempinfo.get('interfacedict')
                        if isinstance(ifdict, list) is False:
                            ifdict = eval(ifdict)
                        logger.debug(ifdict)
                        for obj in ifdict:
                            name = obj.get('name')
                            if name in ['DI','DO']:
                                obj['datatype'] = 'bool'
                            elif name in ['AI','AD','ET','TM','HU','RG']:
                                obj['datatype'] = 'float'
                            elif name in ['arm_state','power_supply','signal_level']:
                                obj['datatype'] = 'int'
                            else:
                                obj['datatype'] = 'string'

                            if 'desc' in obj:
                                obj['desc'] = urllib.quote(obj['desc'])
                            if 'alias' in obj:
                                obj['alias'] = urllib.quote(obj['alias'])
                        templateinfo[tempid] = ifdict

#            logger.debug(retbody)
            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_delete(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei O  
                        vid
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_delete action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('memberid' not in action_body) or  ('vid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['memberid'] is None or action_body['vid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            vid = action_body['vid']
            memberid = action_body['memberid']
            objectid_memberid = ObjectId(memberid)

            searchkey = self.KEY_TOKEN_NAME_ID % ('*', '*', memberid)
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            memberinfo = None
            tnikey = None
            if len(resultlist) == 0:
                memberinfo = self.collect_memberinfo.find_one({'_id':objectid_memberid})
            else:
                tnikey = resultlist[0]
                logger.debug(tnikey)
                memberinfo = self._redis.hgetall(tnikey)

            if memberinfo == None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            if 'device' in memberinfo:
                devicelist = memberinfo['device']
                if isinstance(devicelist,str):
                    devicelist = eval(devicelist)
                for deviceid in devicelist:
                    deviceinfo = self.collect_gearinfo.find_one({'deviceid':deviceid})
                    if deviceinfo is not None:
                        memberlist = deviceinfo['member_id']
                        if memberid in memberlist:
                            memberlist.remove(memberid)
                        self.collect_gearinfo.update_one({'deviceid':deviceid},{'$set':{'member_id':memberlist}})
                        setkey = self.KEY_IMEI_ID %(deviceid,deviceinfo['_id'].__str__())
                        logger.debug(setkey)
                        self._redis.hset(setkey,'member_id',memberlist)
            self.collect_memberinfo.remove({'_id':objectid_memberid})
            if tnikey is not None:
                self._redis.delete(tnikey)

            retdict['error_code'] = self.ERRORCODE_OK

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_add_app(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_add_app action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return


            tid = action_body['tid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            memberid = tni['_id']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            creator = memberinfo['nickname']
            if len(creator) == 0:
                creator = memberinfo['username']

            lastappidinfo_iter = self.collect_appinfo.find({}).limit(1).sort('appid_int',pymongo.DESCENDING)
            appid = 1
            if lastappidinfo_iter.count():
                lastappinfo = lastappidinfo_iter[0]
                appid = lastappinfo['appid_int']+1

            insertdict = dict()
            for key in action_body:
                if key in ['tid']:
                    continue
                else:
                    insertdict[key] = self.parse_param_lowlevel(action_body[key])

            insertdict['ownerid'] = memberid
            insertdict['creator'] = creator
            insertdict['appid'] = appid.__str__()
            insertdict['appid_int'] = appid
            insertdict['createtime'] = datetime.datetime.now()
            memberidlist = list()
            memberidlist.append(memberid)
            insertdict['member_id'] = memberidlist
            insertdict['state'] = self.GEAR_STATE_NEW
            insertresults = self.collect_appinfo.insert_one(insertdict)
            # insertid = self.collect_appinfo.insert_one(insertdict)
            insertid = insertresults.inserted_id

            insertlog = dict({\
                'appid': appid.__str__(),\
                'member_id': memberid,\
                'action': 'add',\
                'device_id':'',\
                'objectid':insertid.__str__(),\
                'tablename':'app_info',\
                'prefix': memberinfo['prefix'],\
                'timestamp': datetime.datetime.now()})
            self.collect_gearlog.insert_one(insertlog)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['appid'] = appid

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_del_app(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_del_app action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or ('appid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['appid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return


            tid = action_body['tid']
            appid = action_body['appid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            memberid = tni['_id']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            appinfo = self.collect_appinfo.find_one({'appid':appid})
            if appinfo is None:
                retdict['error_code'] = self.ERRORCODE_APPID_NOT_EXIST
                return

            if appinfo['ownerid'] != memberid:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_APP_OWNER
                return
            #删除虚拟app
            self.collect_appinfo.remove({'appid':appid})
            #记录log
            insertloglist = list()
            insertlog = dict({\
                'appid': appid,\
                'member_id': memberid,\
                'action': 'del',\
                'objectid':appinfo['_id'].__str__(),\
                'tablename':'app_info',\
                'device_id':'',\
                'prefix': memberinfo['prefix'],\
                'timestamp': datetime.datetime.now()})
            insertloglist.append(insertlog)
            #删除内存key
            searchkey = self.KEY_ALARM_APPID_VDID_DID % (appid,'*','*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)
            searchkey = self.KEY_APPID_VDID_DID % (appid,'*','*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)
            searchkey = self.KEY_ONLINE_APPID_VDID % (appid,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)
            searchkey = self.KEY_APPID_VDID % (appid,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            #删除此app下的所有vdid
            vdeviceinfo_iter = self.collect_vdevice.find({'appid':appid})
            for vdeviceinfo in vdeviceinfo_iter:
                insertlog = dict({\
                    'vdeviceid': vdeviceinfo['vdeviceid'],\
                    'member_id': memberid,\
                    'action': 'del',\
                    'objectid':vdeviceinfo['_id'].__str__(),\
                    'tablename':'vdevice_info',\
                    'device_id':'',\
                    'prefix': memberinfo['prefix'],\
                    'timestamp': datetime.datetime.now()})
                insertloglist.append(insertlog)
            self.collect_vdevice.remove({'appid':appid})
            self.collect_gearlog.insert_many(insertloglist)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['appid'] = appid

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

    def _proc_action_member_add_visual_device(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_add_visual_device action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or ('appid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['appid'] is None:
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return


            tid = action_body['tid']
            appid = action_body['appid']
            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            memberid = tni['_id']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            creator = memberinfo['nickname']
            appinfo = self.collect_appinfo.find_one({'appid':appid})
            if appinfo is None:
                retdict['error_code'] = self.ERRORCODE_APPID_NOT_EXIST
                return

            if appinfo['ownerid'] != memberid:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_APP_OWNER
                return


            lastvdeviceidinfo_iter = self.collect_vdevice.find({}).limit(1).sort('vdeviceid_int',pymongo.DESCENDING)
            vdeviceid = 1
            if lastvdeviceidinfo_iter.count():
                lastvdeviceinfo = lastvdeviceidinfo_iter[0]
                vdeviceid = lastvdeviceinfo['vdeviceid_int']+1

            insertdict = dict()
            for key in action_body:
                if key in ['tid']:
                    continue
                else:
                    insertdict[key] = self.parse_param_lowlevel(action_body[key])

            insertdict['ownerid'] = memberid
            insertdict['creator'] = creator
            insertdict['vdeviceid'] = vdeviceid.__str__()
            insertdict['vdeviceid_int'] = vdeviceid
            insertdict['createtime'] = datetime.datetime.now()
            insertdict['state'] = self.GEAR_STATE_NEW
            memberidlist = list()
            memberidlist.append(memberid)
            insertdict['member_id'] = memberidlist
            insertresults = self.collect_vdevice.insert_one(insertdict)
            # insertid = self.collect_vdevice.insert_one(insertdict)
            insertid = insertresults.inserted_id
            
            insertlog = dict({\
                'vdeviceid': vdeviceid.__str__(),\
                'member_id': memberid,\
                'action': 'add',\
                'device_id':'',\
                'objectid':insertid.__str__(),\
                'tablename':'vdevice_info',\
                'prefix': memberinfo['prefix'],\
                'timestamp': datetime.datetime.now()})
            self.collect_gearlog.insert_one(insertlog)

            #删除老的key，根据node字段中的key 添加新的
            searchkey = self.KEY_APPID_VDID_DID % (appid,vdeviceid.__str__(),'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            if 'nodes' in action_body:
                for deviceinfo in action_body['nodes']:
                    addkey = self.KEY_APPID_VDID_DID % (appid, vdeviceid.__str__(),deviceinfo['deviceid'])
                    self._redis.set(addkey,'1')

            addkey = self.KEY_APPID_VDID % (appid, vdeviceid.__str__())
            self._redis.hmset(addkey,insertdict)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['vdeviceid'] = vdeviceid

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL

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

    def _proc_action_member_update_visual_device(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_update_visual_device action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or ('appid' not in action_body) or ('vdeviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['appid'] is None or action_body['vdeviceid'] is None :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return


            tid = action_body['tid']
            appid = action_body['appid']
            vdeviceid = action_body['vdeviceid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            memberid = tni['_id']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            creator = memberinfo['nickname']
            appinfo = self.collect_appinfo.find_one({'appid':appid})
            if appinfo is None:
                retdict['error_code'] = self.ERRORCODE_APPID_NOT_EXIST
                return

            if appinfo['ownerid'] != memberid:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_APP_OWNER
                return

            vdeviceinfo = self.collect_vdevice.find_one({'vdeviceid':vdeviceid})
            if vdeviceinfo is None:
                retdict['error_code'] = self.ERRORCODE_VDID_NOT_EXIST
                return

            if vdeviceinfo['ownerid'] != memberid:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_VDID_OWNER
                return


            insertdict = dict()
            for key in action_body:
                if key in ['tid']:
                    continue
                else:
                    insertdict[key] = self.parse_param_lowlevel(action_body[key])

            self.collect_vdevice.update_one({'vdeviceid':vdeviceid},{'$set':insertdict})

            insertlog = dict({\
                'vdeviceid': vdeviceinfo['vdeviceid'],\
                'member_id': memberid,\
                'action': 'update',\
                'device_id':'',\
                'objectid':vdeviceinfo['_id'].__str__(),\
                'tablename':'vdevice_info',\
                'prefix': memberinfo['prefix'],\
                'timestamp': datetime.datetime.now()})
            self.collect_gearlog.insert_one(insertlog)

            #删除老的key，根据node字段中的key 添加新的
            searchkey = self.KEY_APPID_VDID_DID % (appid,vdeviceid.__str__(),'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            if 'nodes' in action_body:
                for deviceinfo in action_body['nodes']:
                    addkey = self.KEY_APPID_VDID_DID % (appid, vdeviceid.__str__(),deviceinfo['deviceid'])
                    self._redis.set(addkey,'1')

            addkey = self.KEY_APPID_VDID % (appid, vdeviceid.__str__())
            self._redis.hmset(addkey,insertdict)
            
            retdict['error_code'] = self.ERRORCODE_OK
            retbody['vdeviceid'] = vdeviceid

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = self.ERRORCODE_SERVER_ABNORMAL


    def _proc_action_member_del_visual_device(self, version, action_body, retdict, retbody):
        '''
        input : {    'action_cmd'  : 'logout', M
                     'seq_id      : M
                     'version'    : M
                     'body'   :{
                        tid     tokenid
                        imei    M   设备的IMEI
                        vid M   厂商id
                        relationship    O   
                    }
                }

        output:{   
                   'error_code       : "200"'
                   'seq_id'         : M
                    body:{
                   }
                }
        '''
        logger.debug(" into _proc_action_member_del_visual_device action_body:%s"%action_body)
        try:
#        if 1:
            
            if ('tid' not in action_body) or ('appid' not in action_body) or ('vdeviceid' not in action_body):
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return

            if action_body['tid'] is None or action_body['appid'] is None or action_body['vdeviceid'] is None :
                retdict['error_code'] = self.ERRORCODE_CMD_HAS_INVALID_PARAM
                return


            tid = action_body['tid']
            appid = action_body['appid']
            vdeviceid = action_body['vdeviceid']

            searchkey = self.KEY_TOKEN_NAME_ID % (tid, '*', '*')
            resultlist = self._redis.keys(searchkey)
            logger.debug("resultlist = %r" % (resultlist))
            if len(resultlist) == 0:
                retbody['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            tnikey = resultlist[0]
            logger.debug(tnikey)
            tni = self._redis.hgetall(tnikey)
            if tni is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_TOKEN_OOS
                return

            memberid = tni['_id']
            memberinfo = self.collect_memberinfo.find_one({'_id':ObjectId(memberid)})
            if memberinfo is None:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_EXIST
                return

            appinfo = self.collect_appinfo.find_one({'appid':appid})
            if appinfo is None:
                retdict['error_code'] = self.ERRORCODE_APPID_NOT_EXIST
                return

            if appinfo['ownerid'] != memberid:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_APP_OWNER
                return
            vdeviceinfo = self.collect_vdevice.find_one({'vdeviceid':vdeviceid})
            if vdeviceinfo is None:
                retdict['error_code'] = self.ERRORCODE_VDID_NOT_EXIST
                return

            if vdeviceinfo['ownerid'] != memberid:
                retdict['error_code'] = self.ERRORCODE_MEMBER_NOT_VDID_OWNER
                return

            #删除虚拟app
            self.collect_vdevice.remove({'vdeviceid':vdeviceid})
            #记录log
            insertlog = dict({\
                'vdeviceid': vdeviceid,\
                'member_id': memberid,\
                'action': 'del',\
                'objectid':vdeviceinfo['_id'].__str__(),\
                'tablename':'vdevice_info',\
                'device_id':'',\
                'prefix': memberinfo['prefix'],\
                'timestamp': datetime.datetime.now()})
            self.collect_gearlog.insert_one(insertlog)
            #删除内存key
            searchkey = self.KEY_ALARM_APPID_VDID_DID % ('*',vdeviceid,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)
            searchkey = self.KEY_APPID_VDID_DID % ('*',vdeviceid,'*')
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)
            searchkey = self.KEY_ONLINE_APPID_VDID % ('*',vdeviceid)
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)
            searchkey = self.KEY_APPID_VDID % ('*',vdeviceid)
            resultlist = self._redis.keys(searchkey)
            if len(resultlist):
                self.redisdelete(resultlist)

            retdict['error_code'] = self.ERRORCODE_OK
            retbody['appid'] = appid

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
        memberlogic = MemberLogic(i)
        memberlogic.setDaemon(True)
        memberlogic.start()

    while 1:
        time.sleep(1)
