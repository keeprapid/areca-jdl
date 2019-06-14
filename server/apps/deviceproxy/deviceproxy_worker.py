#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import gevent
from gevent import monkey
from gevent.server import StreamServer
monkey.patch_all()
from multiprocessing import Process
from gevent import socket
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
logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')



class Consumer(threading.Thread):
    """docstring for Consumer"""
    def __init__(self, socketdict):
        super(Consumer, self).__init__()
        self.socketdict = socketdict
        fb = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fb)
        fb.close()

        fb = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fb)
        fb.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.selfqname = 'A:Queue:deviceproxy'
        if self._config is not None and 'deviceproxy' in self._config and self._config['deviceproxy'] is not None:
            if 'Consumer_Queue_Name' in self._config['deviceproxy'] and self._config['deviceproxy']['Consumer_Queue_Name'] is not None:
                self.selfqname = self._config['deviceproxy']['Consumer_Queue_Name']


    def run(self):
        logger.debug("Start Consumer %s" % os.getpid())
        try:
            while 1:
                recvdata = self._redis.brpop(self.selfqname)
                if recvdata:
                    recvbuf = json.loads(recvdata[1])
                    logger.debug(recvbuf)
                    if 'body' in recvbuf:
                        body = recvbuf['body']
                        if 'sockid' in body:
                            sockid = body['sockid']
                            sendbuf = body['sendbuf']
                            if sockid in self.socketdict:
                                sock = self.socketdict[sockid]['sock']
        #                        self.socketdict.pop(recvbuf['sockid'])
#                                recvbuf.pop('sockid')
#                                recvbuf.pop('sender')
                                logger.debug("Send[%s]==>Device" % (sendbuf))
                                sock.sendall(sendbuf)
                                msgdict = dict()
                                msgdict['action_cmd'] = 'ctrl_report'
                                msgdict['body'] = dict()
                                msgdict['body']['report'] = 'send'
                                msgdict['body']['content'] = sendbuf
                                msgdict['body']['deviceid'] = body.get('deviceid')
                                msgdict['body']['ctrlid'] = body.get('ctrlid')
                                msgdict['sockid'] = ''
                                msgdict['from'] = ''
                                msgdict['version'] = '1.0'
                                if 'ctrlproxy' in self._config:
                                    self._redis.lpush(self._config['ctrlproxy']['Consumer_Queue_Name'],json.dumps(msgdict))

        #                        sock.shutdown(socket.SHUT_WR)
        #                        sock.close()
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))

class TCPDogConsumer(threading.Thread):
    """docstring for TCPDogConsumer"""
    def __init__(self, socketdict):
        super(TCPDogConsumer, self).__init__()
        self.socketdict = socketdict
        fb = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
        self._json_dbcfg = json.load(fb)
        fb.close()

        fb = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
        self._config = json.load(fb)
        fb.close()
        self._redis = redis.StrictRedis(self._json_dbcfg['redisip'], int(self._json_dbcfg['redisport']),password=self._json_dbcfg['redispassword'])
        self.selfqname = 'A:Queue:TcpDogWatch'
        if self._config is not None and 'deviceproxy' in self._config and self._config['deviceproxy'] is not None:
            if 'Consumer_Queue_Name_TCP' in self._config['deviceproxy'] and self._config['deviceproxy']['Consumer_Queue_Name_TCP'] is not None:
                self.selfqname = self._config['deviceproxy']['Consumer_Queue_Name_TCP']


    def run(self):
        logger.debug("Start TCPDogConsumer %s" % os.getpid())
        try:
            while 1:
                recvdata = self._redis.brpop(self.selfqname)
                if recvdata:
                    recvbuf = json.loads(recvdata[1])
                    logger.debug("TCPDogConsumer: %r" %(recvbuf))
                    if 'body' in recvbuf:
                        body = recvbuf['body']
                        if 'sockid' in body:
                            sockid = body['sockid']
                            if sockid in self.socketdict:
                                sock = self.socketdict[sockid]['sock']
        #                        self.socketdict.pop(recvbuf['sockid'])
#                                recvbuf.pop('sockid')
#                                recvbuf.pop('sender')
                                logger.debug("close sock %r" % (sock))
                                sock.close()
                                self.socketdict.pop(sockid)
                            searchkey = "A:ds:%s:%s" % ('*',sockid)
                            resultlist = self._redis.keys(searchkey)
                            logger.debug("TCPDogConsumer delete sockkey %r" % (resultlist))
                            for key in resultlist:
                                self._redis.delete(key)
        #                        sock.shutdown(socket.SHUT_WR)
        #                        sock.close()
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))

class Dog2Thread(threading.Thread):

    '''监控responsesocketdict超时请求
       参数如下：
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'requestdatetime']
    '''
    def __init__(self, responsesocketdict, consumer, tcpdogonsumer):  # 定义构造器
        logger.debug("Created Dog2Thread instance")
        self.consumer = consumer
        self.tcpdogconsumer = tcpdogonsumer
        self.responsesocketdict = responsesocketdict
        threading.Thread.__init__(self)

    def run(self):
        logger.debug("Dog2Thread::run")
        while 1:
            if self.consumer is None or self.consumer.is_alive() is False:
                logger.error('Consumer is dead, dog run')
                self.consumer = Consumer(self.responsesocketdict)
                self.consumer.setDaemon(True)
                self.consumer.start()
            else:
                pass

            if self.tcpdogconsumer is None or self.tcpdogconsumer.is_alive() is False:
                logger.error('TCPDogConsumer is dead, dog run')
                self.tcpdogconsumer = TCPDogConsumer(self.responsesocketdict)
                self.tcpdogconsumer.setDaemon(True)
                self.tcpdogconsumer.start()
            else:
                pass

            time.sleep(10)

class DogThread(threading.Thread):

    '''监控responsesocketdict超时请求
       参数如下：
       responsesocketdict:等待响应的socket字典，内容为字典['sock', 'timestamp']
    '''
    def __init__(self, responsesocketdict):  # 定义构造器
        logger.debug("Created DogThread instance")
        threading.Thread.__init__(self)
        self._response_socket_dict = responsesocketdict
        self._timeoutsecond = 10*60

    def run(self):
        logger.debug("DogThread::run")

        while True:
            try:
                now = time.time()

                sortedlist = sorted(self._response_socket_dict.items(), key=lambda _response_socket_dict:_response_socket_dict[1]['timestamp'], reverse = 0)
#                logger.debug(sortedlist)
                for each in sortedlist:
                    key = each[0]
                    responseobj = each[1]
                    requestdatetime = responseobj['timestamp']
                    passedsecond = now - requestdatetime
                    if passedsecond > self._timeoutsecond:
                        sockobj = responseobj['sock']
                        logger.debug("DogThread close timeout sock[%s]%r" %(key, sockobj))
                        sockobj.close()
                        del(self._response_socket_dict[key])
                    else:
                        break
                logger.debug("DogThread finish check in %f" %(time.time()- now))
                time.sleep(60)

            except Exception as e:
                logger.warning("DogThread %s except raised : %s " % (
                    e.__class__, e.args))
                time.sleep(0.1)

def Handle(sock, address):
#    print os.getpid(),sock, address
#    print "%s" % sock.getsockname().__str__()
    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    queuename = "A:Queue:DataCenter"
    if _config is not None and 'datacenter' in _config and _config['datacenter'] is not None:
        if 'Consumer_Queue_Name' in _config['datacenter'] and _config['datacenter']['Consumer_Queue_Name'] is not None:
            queuename = _config['datacenter']['Consumer_Queue_Name']
    selfqueuename = "A:Queue:deviceproxy"
    if _config is not None and 'deviceproxy' in _config and _config['deviceproxy'] is not None:
        if 'Consumer_Queue_Name' in _config['deviceproxy'] and _config['deviceproxy']['Consumer_Queue_Name'] is not None:
            selfqueuename = _config['deviceproxy']['Consumer_Queue_Name']

    sockid = hashlib.md5(address.__str__()).hexdigest()
    if sockid not in socketdict:
        socketinfo = dict()
        socketinfo['sock'] = sock
        socketinfo['timestamp'] = time.time()
        socketdict[sockid] = socketinfo
        logger.debug("New Sock [%s]%r, total count = %d" % (sockid,sock, len(socketdict)))
    recvedbuf = ''
    is_find = False
    is_find_end = False
    key_device_sock = "A:ds:%s:%s"
    while 1:
        try:
            recv = sock.recv(2048)
            logger.debug("recvbuf[%s]%s" % (sockid, recv))
            if len(recv) == 0:
                logger.error("Connection close by peer")
                if sockid in socketdict:
                    socketdict.pop(sockid)
                    logger.debug("Delete Sock, total count = %d" % (len(socketdict)))
                searchkey = key_device_sock % ('*',sockid)
                resultlist = r.keys(searchkey)
                for key in resultlist:
                    r.delete(key)

                sock.close()
                break

            recvedbuf = "%s%s" % (recvedbuf, recv)
            #先找命令头
#            logger.debug('recvedbuf: %s' %(recvedbuf))
            cmdlist = recvedbuf.split('$')
#            print '1 cmdlist = '+cmdlist.__str__()
            unfinishcmdlist = list()
            count = len(cmdlist)
            
            if cmdlist[0] != '':
                cmdlist = cmdlist[1:]

            if cmdlist[-1] != '':
                unfinishcmdlist = cmdlist[-1:]
                cmdlist = cmdlist[:-1]
            else:
                if count>=2 and cmdlist[-2] == '':
                    unfinishcmdlist.append('')


#            logger.debug('2 cmdlist = %s' % (cmdlist.__str__()))
            if len(unfinishcmdlist):
                recvedbuf = '$%s' % unfinishcmdlist[0]
            else:
                recvedbuf = ''
#            print '2 recvedbuf = %s' % (recvedbuf)
            for x in cmdlist:
                if x == '':
                    continue
                else:
                    msgdict = dict()
                    msgdict['action_cmd'] = 'device_upmsg'
                    msgdict['body'] = dict()
                    msgdict['body']['recvedbuf'] = x
                    msgdict['body']['sockid'] = sockid
                    msgdict['sockid'] = ''
                    msgdict['from'] = ''
                    msgdict['version'] = '1.0'
#                    logger.debug(msgdict)
#                    logger.debug(r)
                    r.lpush(queuename,json.dumps(msgdict))


            socketdict[sockid]['timestamp'] = time.time()
        except Exception as e:
            logger.debug("%s except raised : %s " % (e.__class__, e.args))
            sock.close()
            break

if __name__ == "__main__":

    fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
    _json_dbcfg = json.load(fileobj)
    fileobj.close()

    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    r = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])

    queuename = "A:Queue:deviceproxy"
    if _config is not None and 'deviceproxy' in _config and _config['deviceproxy'] is not None:
        if 'Consumer_Queue_Name' in _config['deviceproxy'] and _config['deviceproxy']['Consumer_Queue_Name'] is not None:
            queuename = _config['deviceproxy']['Consumer_Queue_Name']

    listen_port = 8082
    threadcount = 2
    if _config is not None and 'deviceproxy' in _config and _config['deviceproxy'] is not None:
        if 'Listen_Port' in _config['deviceproxy'] and _config['deviceproxy']['Listen_Port'] is not None:
            listen_port = int(_config['deviceproxy']['Listen_Port'])
        if 'thread_count' in _config['deviceproxy'] and _config['deviceproxy']['thread_count'] is not None:
            threadcount = int(_config['deviceproxy']['thread_count'])


#    selfqueuename = "%s:%s" % (queuename, os.getpid())
    socketdict = dict()

    dogAgent = DogThread(socketdict)
    dogAgent.setDaemon(True)
    dogAgent.start()

    dog2Agent = Dog2Thread(socketdict,None, None)
    dog2Agent.setDaemon(True)
    dog2Agent.start()

#    for i in xrange(0, threadcount):
#        consumer = Consumer(queuename, socketdict, r)
#        consumer.setDaemon(True)
#        consumer.start()

    server = StreamServer(("",listen_port), Handle)
    server.serve_forever()


    #server.init_socket()

    #gevent.fork()



#server.start_accepting()
#server._stop_event.wait()


#def serve_forever():
#    print ('starting server')
#    StreamServer(("",8888), Handle).serve_forever()
#     
#number_of_processes = 2
#print 'Starting %s processes' % number_of_processes
#for i in range(number_of_processes):
#    Process(target=serve_forever, args=()).start()
# 
#serve_forever()


#server = StreamServer(("",8888), Handle)
