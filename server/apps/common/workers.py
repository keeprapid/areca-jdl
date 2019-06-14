#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:  asset_main.py
# creator:   jacob.qian
# datetime:  2013-5-31
# Ronaldo wokers类的基类

import json

import logging
#import time
import logging.config
logging.config.fileConfig("/opt/Keeprapid/Areca/server/conf/log.conf")
logger = logging.getLogger('Areca')



class WorkerBase():

    ERROR_RSP_UNKOWN_COMMAND = '{"seq_id":"123456","body":{},"error_code":"40000"}'
    FILEDIR_IMG_HEAD = '/mnt/www/html/Areca/image/'
    #errorcode define
    ERRORCODE_OK = "200"
    ERRORCODE_UNKOWN_CMD = "40000"
    ERRORCODE_SERVER_ABNORMAL = "40001"
    ERRORCODE_CMD_HAS_INVALID_PARAM = '40002'
    ERRORCODE_DB_ERROR = '40003'
    ERRORCODE_MEMBER_USERNAME_ALREADY_EXIST = '41001'
    ERRORCODE_MEMBER_PASSWORD_INVALID = '41002'
    ERRORCODE_MEMBER_NOT_EXIST = '41003'
    ERRORCODE_MEMBER_TOKEN_OOS = '41004'
    ERRORCODE_MEMBER_USER_ALREADY_EXIST = '41005'

    ERRORCODE_MEMBER_USERNAME_INVALID = '41006'
    ERRORCODE_MEMBER_SOURCE_INVALID = '41007'
    ERRORCODE_MEMBER_ALREADY_FOLLOW_GEAR = '41008'
    ERRORCODE_MEMBER_NOT_FOLLOW_GEAR = '41009'
    ERRORCODE_MEMBER_PREFIX_ALREADY_EXIST = '41010'
    ERRORCODE_MEMBER_ADMIN_NOT_EXIST = '41011'
    ERRORCODE_MEMBER_IS_NOT_YOU_CHILD = '41012'
    ERRORCODE_MEMBER_IS_NOT_ACTIVE = '41013'
    ERRORCODE_MEMBER_EMAIL_ALREADY_EXIST = '41014'
    
    ERRORCODE_IMEI_NOT_EXIST = '42001'
    ERRORCODE_IMEI_OUT_MAXCOUNT = '42002'
    ERRORCODE_IMEI_HAS_OWNER = '42003'
    ERRORCODE_IMEI_STATE_OOS = '42004'
    ERRORCODE_MEMBER_ISNOT_IMEI_FOLLOW = '42005'
    ERRORCODE_IMEI_CACHE_ERROR = '42006'
    ERRORCODE_IMEI_HAS_SAME_CMD_NOT_RESPONE = '42007'

    ERRORCODE_APPID_NOT_EXIST = '43001'
    ERRORCODE_MEMBER_NOT_APP_OWNER = '43002'
    ERRORCODE_VDID_NOT_EXIST = '43003'
    ERRORCODE_MEMBER_NOT_VDID_OWNER = '43004'

    MAX_LOCATION_NUMBER = 5
    #member_state
    MEMBER_STATE_INIT_ACTIVE = 0
    MEMBER_STATE_EMAIL_COMFIRM = 1

    MEMBER_PASSWORD_VERIFY_CODE = "abcdef"
    #gear
    AUTH_STATE_NEW = 0
    AUTH_STATE_ONLINE = 1
    AUTH_STATE_DISABLE = 2
    AUTH_STATE_OOS = 3

    GEAR_STATE_UNKNOWN = -1
    GEAR_STATE_NEW = 0
    GEAR_STATE_ACTIVE = 1
    GEAR_STATE_OOS = 2
    GEAR_STATE_ALARM = 3
    GEAR_STATE_DISABLE = 4

    GEAR_ONLINE_TIMEOUT = 480
#    GEAR_ONLINE_TIMEOUT = 20
    CLIENT_ONLINE_TIMEOUT = 600
    UNSEND_MSG_TIMEOUT = 60*60

    GEAR_MAX_FOLLOW_COUNT = 6

    CMD_TYPE_SET_NUMBER = 'SetSosNumber'
    CMD_TYPE_SET_CLOCK = 'SetClock'
    CMD_TYPE_START_LOCATION = 'StartLocation'

    #notify type
    NOTIFY_TYPE_GETBACKPASSWORD = 'GetBackPassword'

    #redis
    KEY_TOKEN_NAME_ID = "A:tni:%s:%s:%s"
    KEY_MEMBER_IMEI = "A:md:%s:%s"
    KEY_IMEI_ID = "A:di:%s:%s"
    KEY_CLIENT_ONLINE_FLAG = "A:oui:%s:%s"
    KEY_TEMPLATE_FLAG = "A:tp:%s"
    KEY_DEVICE_INTERFACE_FLAG = "A:alarm:%s:%s"
    KEY_DEVICE_LASTDATA_FLAG = "A:dl:%s"
    KEY_DEVICE_ONLINE_FLAG = "A:doi:%s"
    KEY_DEVICE_SOCKETID_FLAG = "A:ds:%s:%s"
    KEY_DEVICE_CTRL_CMD_LIST = "A:dcl:%s"
    KEY_DEVICE_CURRENT_CMD = "A:cmd:%s"
    KEY_ALARM_DEVICE_IFNAME_CHANNEL = "A:adic:%s:%s:%s"
    KEY_ALARM_APPID_VDID = "A:aavd:%s:%s"
    KEY_APPID_VDID_DID = "A:avd:%s:%s:%s"
    KEY_APPID_VDID = "A:av:%s:%s"
    KEY_ONLINE_APPID_VDID = "A:oav:%s:%s"
    KEY_ONLINE_APPID_VDID_LIST = "A:oavl"
    KEY_ONLINE_DEVICE_LIST = "A:odl"
    KEY_COMBINATION_DATA_DEVICEID = "A:cbd:%s"

    KEY_DI4_ALARM_FLAG = 'A:DI4ALARM:%s'
    # KEY_AD2_ALARM_FLAG = 'A:AD2ALARM:%s'
    DI4_ALARM_TIMEOUT = 6*60
    KEY_AD2_CREATED_ALARM = 'A:AD2CREATEDALARM:%s'
    KEY_DI4AD2_ALARM_TIMEOUT = 'A:AD2DI4ALARMTIMEOUT'
    KEY_DI4AD2_ALARM_DAY_FLAG = "A:AD2DI4DAYFLAG:%s:%s"

    #email
    SEND_EMAIL_INTERVAL = 2

    #special
    ADMIN_VID = "888888888888"

    #location
    LOCATION_TYPE_GPS = '1'
    LOCATION_TYPE_LBS = '2'

    GEAR_ALARM_INVALID = 0
    GEAR_ALARM_CREATE = 1
    GEAR_ALARM_RESTORE = 2
    
#    DOG_SLEEP_TIME = 10
    DOG_SLEEP_TIME = 60
    CTRL_QUEUE_TIMEOUT = 60*60
    CTRL_CMD_TIMEOUT = 60
    
    SPECIAL_APPLICATION_NORMAL = 0
    SPECIAL_APPLICATION_THBIND = 1

    ALARM_TYPE_INTERFACE_ALARM = 'ifalarm'
    ALARM_TYPE_INTERFACE_RESTORE = 'ifrestore'
    ALARM_TYPE_DEVICE_ALARM = 'devicealarm'
    ALARM_TYPE_DEVICE_ACTIVE = 'deviceactive'
    ALARM_TYPE_DEVICE_OOS = 'deviceoos'
    ALARM_TYPE_VDEVICE_ALARM = 'vdevicealarm'
    ALARM_TYPE_VDEVICE_ACTIVE = 'vdeviceactive'
    ALARM_TYPE_VDEVICE_OOS = 'vdeviceoos'
    ALARM_TYPE_INTERFACE_DISABLE = "ifdisabel"
    ALARM_TYPE_INTERFACE_ENABLE = "ifenable"
    ALARM_TYPE_DEVICE_MAINTENANCE = 'devicemaintenance'

    #报警级别
    ALARM_LEVEL_INTERFACE = 'interface'
    ALARM_LEVEL_DEVICE = 'device'
    ALARM_LEVEL_VDEVICE = 'vdevice'

    def __init__(self):
        logger.debug("WorkerBase:__init__")
 
    def redisdelete(self, argslist):
#        logger.debug('%s' % ('","'.join(argslist)))
        ret = eval('self._redis.delete("%s")'%('","'.join(argslist)))
#        logger.debug('delete ret = %d' % (ret))

    def _sendMessage(self, to, body):
        #发送消息到routkey，没有返回reply_to,单向消息
#        logger.debug(to +':'+body)
        if to is None or to == '' or body is None or body == '':
            return

        self._redis.lpush(to, body)

