#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

import paho.mqtt.client as mqtt
import time
import sys


if __name__ == "__main__":

	clientid = sys.argv[1]
	topic = sys.argv[2]
	qos = int(sys.argv[3])
	client = mqtt.Client()
	client.connect("localhost", 2883, 60)
	idx = 0
	print client.publish(topic,"generalcmd,SOS,13760332027,,,MONITOR,13760332027,,FRIEND,13760332027,,,,#")

	while 1:
#		print client.publish(topic,"[%s] nowtime:%d" %(clientid, idx), qos)

#		idx += 1
		time.sleep(0.5)

