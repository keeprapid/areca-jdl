#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

import paho.mqtt.client as mqtt
import sys

if __name__ == "__main__":

	topic = sys.argv[1]
#	clientid = sys.argv[2]
	def on_connect(client, userdata, flags, rc):  
	    print("Connected with result code "+str(rc)) 
	    print(flags)
	    client.subscribe(topic)

	def on_message(client, userdata, msg):
	    print(msg.topic+" "+str(msg.payload))


	client = mqtt.Client()
	client.on_connect = on_connect
	client.on_message = on_message
	client.connect("localhost", 2883, 60)

	client.loop_forever() 

