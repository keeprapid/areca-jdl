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


def on_connect(client, userdata, flags, rc):
    try: 
        logger.debug("Connected with result code "+str(rc))
        logger.debug("userdata = %r " % (userdata))
        if 'topic' in userdata and userdata['topic'] is not None:
            topic = userdata['topic']
            if isinstance(topic, unicode):
                topic = topic.encode('ascii')
            client.subscribe(topic)
    except Exception as e:
        logger.error("%s except raised : %s " % (e.__class__, e.args))


def on_message(client, userdata, msg):
    logger.debug(msg.topic+" "+str(msg.payload))
    try:
        if 'redis' in userdata and userdata['redis'] is not None:
            r = userdata['redis']
            routekey = "W:Queue:MqttProxy"
            r.lpush(routekey, msg.payload)

    except Exception as e:
        logger.error("%s except raised : %s " % (e.__class__, e.args))

class MqttConsumer(threading.Thread):
    """MqttConsumer"""
    def __init__(self, on_message, on_connect):
        super(MqttConsumer, self).__init__()
        self.on_message = on_message
        self.on_connect = on_connect

    def run(self):
        logger.debug("Start consumer")
        try:
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
            userdata = dict()
            userdata['topic'] = _mqtt_cfg['mqtt_listen_topic']
            userdata['redis'] = _redis

            mqttclient = mqtt.Client(userdata = userdata)
            mqttclient.on_connect = self.on_connect
            mqttclient.on_message = self.on_message
            mqttclient.connect(_mqtt_cfg['mqtt_server'], int(_mqtt_cfg['mqtt_port']), int(_mqtt_cfg['mqtt_client_timeout']))

            mqttclient.loop_forever() 
        except Exception as e:
            logger.error("%s except raised : %s " % (e.__class__, e.args))


if __name__ == "__main__":

    obj = MqttConsumer(on_message, on_connect)
    obj.setDaemon(True)
    obj.start()

    while 1:
        time.sleep(1)



