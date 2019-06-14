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

r = redis.StrictRedis()
queuename = "Logic.test"
selfqname = "TestServer"

socketdict = dict()

class Consumer(threading.Thread):
    """docstring for Consumer"""
    def __init__(self, selfqname, socketdict):
        super(Consumer, self).__init__()
        self.selfqname = selfqname
        self.socketdict = socketdict
        self._redis = redis.StrictRedis()


    def run(self):
        print "Start Consumer %s" % os.getpid()
        try:
            while 1:
                recvdata = self._redis.brpop("%s:%s" % (self.selfqname, os.getpid()))
                if recvdata:
#                    print os.getpid(),recvdata
                    recvbuf = json.loads(recvdata[1])
                    if 'sockid' in recvbuf:
                        if recvbuf['sockid'] in self.socketdict:
                            sock = self.socketdict[recvbuf['sockid']]
    #                        self.socketdict.pop(recvbuf['sockid'])
                            recvbuf.pop('sockid')
                            recvbuf.pop('sender')
                            sock.sendall(json.dumps(recvbuf))
    #                        sock.shutdown(socket.SHUT_WR)
    #                        sock.close()
        except Exception as e:
            print ("%s except raised : %s " % (e.__class__, e.args))


        

def Handle(sock, address):
#    print os.getpid(),sock, address
#    print "%s" % sock.getsockname().__str__()
    sockid = hashlib.md5(address.__str__()).hexdigest()
    if sockid not in socketdict:
        socketdict[sockid] = sock
        print "New Sock, total count = %d" % (len(socketdict))
    while 1:
        try:
            recv = sock.recv(1024)
            print recv
            if len(recv) == 0:
                print "Connection close by peer"
                if sockid in socketdict:
                    socketdict.pop(sockid)
                    print "Delete Sock, total count = %d" % (len(socketdict))
                sock.close()
                break
            msgdict = dict()
            msgdict = json.loads(recv)
            msgdict['sockid'] = sockid
            msgdict['sender'] = "%s:%s" % (selfqname, os.getpid())
    
            r.lpush(queuename,json.dumps(msgdict))
        except Exception as e:
            print("%s except raised : %s " % (e.__class__, e.args))

#    sock.sendall('HTTP/1.0 200 OK\n\nOK')
#    sock.shutdown(socket.SHUT_WR)
#    sock.close()
#    break


server = StreamServer(("",8888), Handle)
#server.init_socket()

#gevent.fork()


for i in xrange(0, 2):
    consumer = Consumer(selfqname, socketdict)
    consumer.setDaemon(True)
    consumer.start()

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



server.serve_forever()

