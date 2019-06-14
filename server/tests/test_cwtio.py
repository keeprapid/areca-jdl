#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import socket
import sys
import time



if __name__ == "__main__":

	deviceid = sys.argv[1]
	content = sys.argv[2]
	sendbuf = '$%s%s' % (deviceid, content)
	print sendbuf	
	address = ('115.29.47.171', 8082)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	s.sendall(sendbuf)
	while 1:
#		print client.publish(topic,"[%s] nowtime:%d" %(clientid, idx), qos)

#		idx += 1
		time.sleep(0.5)

