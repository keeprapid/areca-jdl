#! /usr/bin/python
# -*- coding: UTF-8 -*-
# filename:  MC_main.py
# creator:   jacob.qian
# datetime:  2013-1-3


import sys
import os
import json
import time
import subprocess
import logging
import logging.config
import urllib
import redis
logging.config.fileConfig("./conf/log.conf")
logger = logging.getLogger('Areca')

class ModuleControl:

    def __init__(self):
#		logger.debug("ModuleControl::__init__")
        fileobj = open('./conf/db.conf', 'r')
        self._json_dbcfg = json.load(fileobj)
        fileobj.close()
        fileobj = open('./conf/config.conf', 'r')
        self._config = json.load(fileobj)
        fileobj.close()
        self.recv_queue_name = "A:Queue:ModuleMonitor"
        if 'ModuleMonitor' in self._config:
            if 'Consumer_Queue_Name' in self._config['ModuleMonitor']:
                self.recv_queue_name = self._config['ModuleMonitor']['Consumer_Queue_Name']

            
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.projectdir = os.getcwd()


    def startall(self):
        '''启动所有模块'''
        applist = os.listdir('./apps')
        for app in applist:
            if app != 'common':
                self.startapp(app)


    def startapp(self, appname):
        logger.debug("launch %s now...." %(appname))
        fileobj = open('./conf/config.conf', 'r')
        self._config = json.load(fileobj)
        fileobj.close()

        mode = '1'
        launchfile = '%s_main.py' % (appname)
        subprocess_number = 1
        if appname in self._config:
            if 'mode' in self._config[appname]:
                mode = self._config[appname]['mode']
            if 'launchfile' in self._config[appname]:
                launchfile = self._config[appname]['launchfile']
            if 'subprocess_number' in self._config[appname]:
                subprocess_number = int(self._config[appname]['subprocess_number'])


        cmdstr = ''
        if mode == '1':
            cmdstr = 'python %s/apps/%s/%s' % (self.projectdir, appname, launchfile)
        else:
            cmdstr = url

        logger.debug(cmdstr)
        for i in xrange(0, subprocess_number):
            p = subprocess.Popen(cmdstr, shell=True)


    def stopapp(service_name, service_pid):
        logger.debug("stop %s:%s now...." %(appname,service_pid))
        try:
            searchkey = "%s/apps/%s" % (self.projectdir, service_name)
            pidlist = list()
            for line in os.popen("ps -ef|grep '%s'" % (searchkey)).readlines():
                fieldlist = line.split(' ')
                psList = list()
                if 'python' not in fieldlist:
                    continue

                fieldCnt = 0
                for field in fieldlist:
                    if(field != ''):
                        field = field.strip()
                        psList.append(field)
                        fieldCnt = fieldCnt + 1
                if service_pid is not None:
                    targetpid = int(service_pid)
                    if psList[1] == targetpid:
                        pidlist.append(psList[1])
                        break
                else:
                    pidlist.append(psList[1])

            if len(pidlist):
                pidstr = ' '.join(pidlist)
                cmd = "kill -9 %s" % pidstr
                logger.debug(cmd)
                os.popen(cmd)

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))

    def _isalive(self, service_name):
        logb.warning("ModuleControl::isalive service_name = %s" % (
            service_name))
        pidlist = list()
        matchMode = '1'
        launchfile = '%s_main.py' % (service_name)
        if service_name in self._config:
            if 'matchMode' in self._config[service_name]:
                matchMode = self._config[service_name]['matchMode']
            if 'launchfile' in self._config[service_name]:
                launchfile = self._config[service_name]['launchfile']

        searchkey = "%s/apps/%s/%s" % (self.projectdir, service_name, launchfile)
        for line in os.popen("ps -ef|grep '%s'" % (searchkey)).readlines():
            fieldlist = line.split(' ')
            psList = list()
            if 'python' not in fieldlist:
                continue

            fieldCnt = 0
            for field in fieldlist:
                if(field != ''):
                    field = field.strip()
                    psList.append(field)
                    fieldCnt = fieldCnt + 1

            pidlist.append(psList[1])

        return pidlist

    def _startconsumer(self):
        logger.debug("start consumer[%s:%s]" % (self._json_dbcfg['redisip'], self.recv_queue_name))
        while 1:
            try:
                recvdata = self._redis.brpop(self.recv_queue_name)
                recvbuf = recvdata[1]
                self._proc_message(recvbuf)

            except Exception as e:
                logger.error("%s except raised : %s " % (e.__class__, e.args))
                time.sleep(1)



    def kickoff(self):
        '''启动整个moudulemanager，先把任务拉起来，然后在创建ampq队列'''
        #启动进程
        self.startall()
        # 启动接收消息功能
        self._startconsumer()

    def _sendMessage(self, to, body):
        #发送消息到routkey，没有返回reply_to,单向消息
        self._redis.lpush(to, body)


    def _proc_message(self, recvbuf):
        '''消息处理入口函数'''
        logger.debug('_proc_message')
        #解body
        msgdict = dict()
        try:
            print body
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

        if "action_cmd" not in msgdict:
            logger.error("no action_cmd in msg")
            self._sendMessage(msgfrom, '{"from":%s,"error_code":"40000","seq_id":%s,"body":{})' % (self.recv_queue_name, seqid))
            return
        #构建回应消息结构
        action_cmd = msgdict['action_cmd']

        message_resp_dict = dict()
        message_resp_dict['from'] = self.recv_queue_name
        message_resp_dict['seq_id'] = seqid
        message_resp_dict['sockid'] = msgdict['sockid']
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
#            logger.debug('action_params : %s' % (action_params))
        else:
            action_body = None
            logger.debug('no action_body')
        if action_cmd == 'start_service':
            self._proc_action_startservice(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'stop_service':
            self._proc_action_stopservice(action_version, action_body, msg_out_head, msg_out_body)
        elif action_cmd == 'service_state':
            self._proc_action_servicestate(action_version, action_body, msg_out_head, msg_out_body)
        else:
            msg_out_head['error_code'] = '40000'

        return

    def _proc_action_stopservice(self, version, action_body, retdict, retbody):
        '''处理用户开户消息'''
        logger.debug("_proc_action_stopservice")
        #检查参数
        if action_body is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = '40002'
            return
        if 'service_name' not in action_body or action_body['service_name'] is None:
            logger.error("mandotry param error in action stop_service")
            retdict['error_code'] = '40002'
            return


        try:
            service_name = action_params['service_name']
            service_pid = None
            if 'pid' in action_body and action_body['pid'] is not None:
                service_pid = action_body['pid']

            ret = self.stopapp(service_name, service_pid)
            retdict['error_code'] = ret

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return


    def _proc_action_startservice(self, version, action_body, retdict, retbody):
        '''处理用户开户消息'''
        logger.debug("_proc_action_startservice")
        #检查参数
        if action_body is None:
            logger.error("mandotry param error in action start_service")
            retdict['error_code'] = '40002'
            return
        if 'service_name' not in action_body or action_body['service_name'] is None:
            logger.error("mandotry param error in action start_service")
            retdict['error_code'] = '40002'
            return


        try:
            service_name = action_body['service_name']
            self.startapp(service_name)
            retdict['error_code'] = '200'
            return
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['error_code'] = '40001'
            return

    def _proc_action_servicestate(self, version, action_body, retdict, retbody):
        '''处理用户开户消息'''
        logger.debug("_proc_action_servicestate")
        try:
            service_name = None
            if action_body is not None and 'service_name' in action_body and action_body['service_name'] is not None:
                service_name = action_body['service_name']

            servicelist = list()
            retbody['service_list'] = servicelist

            if service_name:
                pidlist = self._isalive(service_name)
                if len(pidlist):
                    for pid in pidlist:
                        serviceinfo = dict()
                        serviceinfo['service_name'] = service_name
                        serviceinfo['service_pid'] = pid
                        serviceinfo['service_state'] = '1'
                        servicelist.append(serviceinfo)
                else:
                    serviceinfo = dict()
                    serviceinfo['service_name'] = service_name
                    serviceinfo['service_pid'] = -1
                    serviceinfo['service_state'] = '0'
            else:
                applist = os.listdir('./apps')
                for app in applist:
                    if app == 'common':
                        continue

                    pidlist = self._isalive(app)
                    if len(pidlist):
                        for pid in pidlist:
                            serviceinfo = dict()
                            serviceinfo['service_name'] = app
                            serviceinfo['service_pid'] = pid
                            serviceinfo['service_state'] = '1'
                            servicelist.append(serviceinfo)
                    else:
                        serviceinfo = dict()
                        serviceinfo['service_name'] = app
                        serviceinfo['service_pid'] = -1
                        serviceinfo['service_state'] = '0'

            retdict['error_code'] = '200'

        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            retdict['errorcode'] = '401'
            return



if __name__ == "__main__":

#    if(len(sys.argv) < 2):
#        logger.error("need param idx ")
#        sys.exit()

    modu = ModuleControl()
    modu.kickoff()
