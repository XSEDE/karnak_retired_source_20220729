
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

logging.basicConfig(level=logging.ERROR)

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
    print("   job: "+routing_key)
    #raise Exception("testing no ack!")

def queueUpdate(consumer_tag, routing_key, exchange, content):
    print(" queue: "+routing_key)

def systemUpdate(consumer_tag, routing_key, exchange, content):
    print("system: "+routing_key)

def connectXsedeCarnac():
    print("connecting to carnac queues")
    conn = _connectSecure("info1.dyn.teragrid.org","xsede")
    channel = conn.channel()
    #print("  archive.activity queue")
    channel.basicConsume(jobUpdate,"carnac.archive.activity",no_ack=False)

    #print("  consume from archive queue")
    channel.basicConsume(queueUpdate,"carnac.archive",no_ack=False)

    #print("  consume from sysinfo queue")
    channel.basicConsume(systemUpdate,"carnac.sysinfo",no_ack=False)

def connectXsede():
    conn = _connectSecure("info1.dyn.teragrid.org","xsede")
    channel = conn.channel()

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.computing_activity","#")
    channel.basicConsume(jobUpdate,queue)

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.computing_activities","#")
    channel.basicConsume(queueUpdate,queue)

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.compute","#")
    channel.basicConsume(systemUpdate,queue)

def connectXsedeOld():
    conn = _connectSecure("info1.dyn.teragrid.org","xsede_private")
    channel = conn.channel()

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.computing_activity","#")
    channel.basicConsume(jobUpdate,queue)

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.computing_activities","#")
    channel.basicConsume(queueUpdate,queue)

    conn = _connectSecure("info1.dyn.teragrid.org","xsede_public")
    channel = conn.channel()

    queue = channel.queueDeclare()
    channel.queueBind(queue,"glue2.compute","#")
    channel.basicConsume(systemUpdate,queue)
    
if __name__ == "__main__":

    #connectXsedeCarnac()
    #connectXsedeOld()
    connectXsede()

    print("waiting for messages")
    while True:
        time.sleep(1)
    print("done")
