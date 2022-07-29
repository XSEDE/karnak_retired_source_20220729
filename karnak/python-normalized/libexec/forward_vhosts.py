
import copy
import datetime
import json
import logging
import logging.config
import os
import ssl
import sys
import time
import traceback
import xml.dom.minidom

from mtk.amqp_0_9_1 import *

#######################################################################################################################


def _connect(host, vhost):
    return Connection(host=host,
                      port=5672,
                      virtual_host=vhost,
                      mechanism=PlainMechanism("carnac","98rid9%82h8f99)82rh8uh"),
                      heartbeat=60)

def _connectSecure(host, vhost):
    ssl_options = {"keyfile": "/home/karnak/cert/serverkey.pem",
                   "certfile": "/home/karnak/cert/servercert.pem",
                   "cert_reqs": ssl.CERT_REQUIRED,
                   "ca_certs": "/home/karnak/cert/cacerts.pem"}

    return Connection(host=host,
                      port=5671,
                      virtual_host=vhost,
                      mechanism=X509Mechanism(),
                      ssl_options=ssl_options,
                      heartbeat=60)

#######################################################################################################################

def jobUpdate(consumer_tag, routing_key, exchange, content):
    print("  job: "+routing_key)

def queueUpdate(consumer_tag, routing_key, exchange, content):
    print("queue: "+routing_key)
    
if __name__ == "__main__":
    conn = _connectSecure("info1.dyn.teragrid.org","xsede_private")
    channel = conn.channel()
    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.computing_activity","#")
    #channel.queueBind(queue,"glue2.computing_activity","#.stampede.tacc.xsede.org")
    channel.basicConsume(jobUpdate,queue)

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.computing_activities","#")
    channel.basicConsume(queueUpdate,queue)

    while True:
        time.sleep(1)
