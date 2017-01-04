#!/usr/bin/env python

import os
import argparse
import tornado
import tornado.web
import urllib2
import thread
import threading
import time
import json
import subprocess
from jinja2 import Template

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class MainHandler(tornado.web.RequestHandler):
    
    def initialize(self, mainServer, **kwds):
        self.mainServer = mainServer

    def get(self):        
        self.write("Hello")


class ProxyHandler(tornado.web.RequestHandler):
    
    def initialize(self, mainServer, **kwds):
        self.mainServer = mainServer
        self.url = mainServer + "/gaia/vertex/query"

    def post(self):   
        payload = self.request.body
        print "Proxy to %s request: %s" %(self.url, payload)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        request = urllib2.Request(self.url, payload, headers=headers)
        response = urllib2.urlopen(request)
        result = response.read()
        self.write(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("main")
    parser.add_argument("--port", type=int, default=8080)

    args = parser.parse_args()

    config = {
        'mainServer' : args.main
    }
    
    application = tornado.web.Application([
        #(r"/", MainHandler, config),
        (r"/gaia/vertex/query", ProxyHandler, config),
        (r"/static/(.*)", tornado.web.StaticFileHandler, dict(path=STATIC_DIR) )
    ])

    application.listen(args.port)
    tornado.ioloop.IOLoop.instance().start()
