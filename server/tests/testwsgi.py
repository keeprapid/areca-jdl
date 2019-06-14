#!/usr/bin python
# -*- coding: UTF-8 -*-
# filename:   ipserver_main.py
# creator:   jack.li
# datetime:  2014-8-18
# Ronaldo  ip 主文件

from gevent import monkey
monkey.patch_all()
from gevent import wsgi

def app(env, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    return ['<html><head><title>AB Test</title></head><body><h1>Hello</h1></body></html>']

print('Start gevent 8888...')
 
a = wsgi.WSGIServer(('', 8888),app,log=None)

a.serve_forever()