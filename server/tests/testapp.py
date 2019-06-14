#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

import socket
import time
import threading
import sys
import redis
import json


#r = redis.StrictRedis()
selfqname = "Logic.test"

class Server(threading.Thread):
	"""docstring for Server"""
	def __init__(self, selfqname):
		super(Server, self).__init__()
		self.selfqname = selfqname
		self._redis = redis.StrictRedis()

	def run(self):
		print "start consumer"
		while 1:
			recvdata = self._redis.brpop(self.selfqname)
			if recvdata:
#				print recvdata
				recvbuf = json.loads(recvdata[1])
				if 'sender' in recvbuf:
					sendto = recvbuf['sender']
					recvbuf['sender'] = self.selfqname
					self._redis.lpush(sendto, json.dumps(recvbuf))

		
for i in xrange(0,5):
	consumer = Server(selfqname)
	consumer.setDaemon(True)
	consumer.start()

while 1:
	time.sleep(1)




