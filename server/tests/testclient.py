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
import json
import random



class RecvThread(threading.Thread):
	def __init__(self, sock, client):  # 定义构造器
		threading.Thread.__init__(self)
		self.sock = sock
		self.client = client

	def run(self):
		print "in recv thread %d , sock = %r" % (self.client.index, self.sock)
		while 1:
			try:
				data = self.sock.recv(1024)
				if len(data) == 0:
					self.sock.close()

#				print data
				data = json.loads(data)
				if 'seqid' in data:
					seqid = int(data['seqid'])
	#					print seqid,seqid.__class__
	#					print self.client.extime
					t = self.client.extime.pop(seqid)
	#					print t,t.__class__
					print "[%d] get %d respone, extime = %f" % (self.client.index, seqid, (time.time()-t))

					
			except Exception as e:
				print ("%s except raised : %s " % (e.__class__, e.args))

class client(threading.Thread):
	"""docstring for client"""
	def __init__(self, index):
		super(client, self).__init__()
		self.seqid = 0
		self.index = index
		self.extime = dict()

	def run(self):
		address = ('120.24.63.184', 8888)
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect(address)
		except:
			time.sleep(5)
			s.close()
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect(address)

		recver = RecvThread(s, self)
		recver.setDaemon(True)
		recver.start()

		while 1:
			try:
				senddata = dict()
				senddata['seqid'] = "%d" % self.seqid
				senddata['data'] = "123"
				s.send(json.dumps(senddata))
				t = time.time()
				self.extime[self.seqid] = t

#				print "[%d]send data seqid = %s, time = %s" % (self.index, self.seqid,t)
				self.seqid += 1
				time.sleep(random.randint(2,10))
			except:
				time.sleep(5)
				s.close()
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect(address)


		
if __name__ == "__main__":
	threadcount = 1

	if len(sys.argv)>1 and sys.argv[1] is not None:
		threadcount = int(sys.argv[1])

	for i in xrange(0, threadcount):
		c = client(i)
		c.setDaemon(True)
		c.start()

	while 1:
		time.sleep(5)



