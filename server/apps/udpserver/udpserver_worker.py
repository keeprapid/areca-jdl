#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

import socket
import redis
import sys
import time
import json
import threading
import os
import logging
import logging.config
logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')


class UdpServer(threading.Thread):
    def __init__(self, selfqname, udpport, redisconn):
        super(UdpServer, self).__init__()
        self.selfqname = selfqname
        self.udpport = udpport
        self._redis = redisconn


    def run(self):

        queuename = "A:Queue:DataCenter"
        if _config is not None and 'datacenter' in _config and _config['datacenter'] is not None:
            if 'Consumer_Queue_Name' in _config['datacenter'] and _config['datacenter']['Consumer_Queue_Name'] is not None:
                queuename = _config['datacenter']['Consumer_Queue_Name']
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
        sock.bind(('', self.udpport))       
        # 绑定同一个域名下的所有机器  
          
        while True:
            try:
                revcData, (remoteHost, remotePort) = sock.recvfrom(1024)  
                logger.debug("[%s:%s] connect" % (remoteHost, remotePort))
                logger.debug("recvdata = %s" % revcData)
                if revcData is not None and len(revcData)>0:
                    recvbuf = revcData.replace("$",'')
                    msgdict = dict()
                    msgdict['action_cmd'] = 'device_upmsg'
                    msgdict['body'] = dict()
                    msgdict['body']['recvedbuf'] = recvbuf
                    msgdict['body']['sockid'] = ''
                    msgdict['sockid'] = ''
                    msgdict['from'] = ''
                    msgdict['version'] = '1.0'
#                    logger.debug(msgdict)
                    self._redis.lpush(queuename,json.dumps(msgdict))
            except Exception as e:
                logger.debug("%s except raised : %s " % (e.__class__, e.args))

                  
        sock.close()  
        

if __name__ == "__main__":

    fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
    _json_dbcfg = json.load(fileobj)
    fileobj.close()

    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    r = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])

    queuename = "A:Queue:UdpServer"
    if _config is not None and 'udpserver' in _config and _config['udpserver'] is not None:
        if 'Consumer_Queue_Name' in _config['udpserver'] and _config['udpserver']['Consumer_Queue_Name'] is not None:
            queuename = _config['udpserver']['Consumer_Queue_Name']

    listen_port = 18082
    if _config is not None and 'udpserver' in _config and _config['udpserver'] is not None:
        if 'Listen_Port' in _config['udpserver'] and _config['udpserver']['Listen_Port'] is not None:
            listen_port = int(_config['udpserver']['Listen_Port'])

    udpServer = UdpServer(queuename, listen_port, r)
    udpServer.setDaemon(True)
    udpServer.start()  
    while 1:
        time.sleep(1)
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
