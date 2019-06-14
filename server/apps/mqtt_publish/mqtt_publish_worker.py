#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件
import paho.mqtt.client as mqtt

from multiprocessing import Process
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

class PublishDog(threading.Thread):
    """docstring for PublishDog"""
    def __init__(self, client, keepAlive):
        super(PublishDog, self).__init__()
        self.client = client
        self.keepAlive = keepAlive
        self.lasttime = time.time()

    def run(self):
        logger.debug("Start dog")
        try:
            while 1:
                if (time.time() - self.lasttime) > self.keepAlive:
                    logger.debug("Dog run, send ping")
                    self.client._send_pingreq()
                    self.lasttime = time.time()

                time.sleep(1)
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))


def msg_proc(recvbuf, client):
 #   logger.debug(recvbuf)
#    try:
    msgbody = json.loads(recvbuf)

    if 'topic' not in msgbody or msgbody['topic'] is None or 'sendbuf' not in msgbody or msgbody['sendbuf'] is None:
        logger.error("recvbuf format error!!!")
        return
    sendbuf = msgbody['sendbuf']
    if isinstance(sendbuf, unicode):
        sendbuf = sendbuf.encode('ascii')
    topic = msgbody['topic']
    if isinstance(topic, unicode):
        topic = topic.encode('ascii')


    client.publish(topic, sendbuf)

#    except Exception as e:
#        logger.error("%s except raised : %s " % (e.__class__, e.args))


if __name__ == "__main__":

    fileobj = open("/opt/Keeprapid/Areca/server/conf/config.conf", "r")
    _config = json.load(fileobj)
    fileobj.close()
    fileobj = open('/opt/Keeprapid/Areca/server/conf/db.conf', 'r')
    _json_dbcfg = json.load(fileobj)
    fileobj.close()
    fileobj = open('/opt/Keeprapid/Areca/server/conf/mqtt.conf', 'r')
    _mqtt_cfg = json.load(fileobj)
    fileobj.close()

    _redis = redis.StrictRedis(_json_dbcfg['redisip'], int(_json_dbcfg['redisport']),password=_json_dbcfg['redispassword'])
    recv_queue_name = "W:Queue:MQTTPub"
    if 'mqtt_publish' in _config:
        if 'Consumer_Queue_Name' in _config['mqtt_publish']:
            recv_queue_name = _config['mqtt_publish']['Consumer_Queue_Name']

    mqttclient = mqtt.Client()
    mqttclient.connect(_mqtt_cfg['mqtt_server'], int(_mqtt_cfg['mqtt_port']), int(_mqtt_cfg['mqtt_client_timeout']))

    dog = PublishDog(mqttclient, int(_mqtt_cfg['mqtt_client_timeout'])/2) 
    dog.setDaemon(True)
    dog.start()
    logger.debug("mqtt_publish_worker kickoff... "+recv_queue_name)
    while 1:
        try:
            recvdata = _redis.brpop(recv_queue_name)
            msg_proc(recvdata[1], mqttclient)
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))
            mqttclient = mqtt.Client()
            mqttclient.connect(_mqtt_cfg['mqtt_server'], int(_mqtt_cfg['mqtt_port']), int(_mqtt_cfg['mqtt_client_timeout']))
            dog.client = mqttclient





        