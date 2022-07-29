
import calendar
import datetime
import json
import logging
import logging.config
import os
import ssl
import sys
import time
import traceback
import xml.dom
import xml.dom.minidom

from ipf.daemon import Daemon
from mtk.amqp_0_9_1 import *

from carnac.database import Glue2Database
from carnac.job import *

libexec_path = os.path.split(os.path.abspath(__file__))[0]
carnac_path = os.path.split(libexec_path)[0]

logging.config.fileConfig(os.path.join(carnac_path,"etc","logging.conf"))

logger = logging.getLogger("carnac.sysinfo")
    
#######################################################################################################################

def getText(doc, tag):
    nodes = doc.getElementsByTagName(tag)
    if len(nodes) < 1:
        return ""
    if nodes[0].firstChild == None:
        return ""
    if nodes[0].firstChild.data == None:
        return ""
    return nodes[0].firstChild.data

def getAttribute(doc, element, attribute):
    nodes = doc.getElementsByTagName(element)
    if len(nodes) < 1:
        return ""
    return nodes[0].getAttribute(attribute)

def convertToEpoch(dateTime):
    # 2009-06-04T17:44:02Z
    dateTime = dateTime[:len(dateTime)-1]+" UTC"
    dt = datetime.datetime.strptime(dateTime,"%Y-%m-%dT%H:%M:%S %Z")
    return calendar.timegm(dt.timetuple())

#######################################################################################################################

class SysInfo(Daemon):
    def __init__(self):
        Daemon.__init__(self,
                        pidfile=os.path.join(carnac_path,"var","sysinfo.pid"),
                        stdout=os.path.join(carnac_path,"var","sysinfo.log"),
                        stderr=os.path.join(carnac_path,"var","sysinfo.log"))
        self.connected = False
        
    def run(self):
        while True:
            if not self.connected:
                try:
                    self._connect("info1.dyn.xsede.org")
                except MtkError:
                    traceback.print_exc()
                    self._connect("info2.dyn.xsede.org")
            time.sleep(15)

    def _connect(self, host):
        logger.info("connecting to %s" % host)
        ssl_options = {"cert_reqs": ssl.CERT_REQUIRED,
                       "ca_certs": "/home/karnak/cert/cacerts.pem"}
        self.xsede_connection = Connection(host=host,
                                           port=5671,
                                           virtual_host="xsede",
                                           mechanism=PlainMechanism("carnac","y3S7pdmAYzF3RHuE"),
                                           ssl_options=ssl_options,
                                           heartbeat=60,
                                           closed_callback=self._closed)
        self.xsede_channel = self.xsede_connection.channel()
        self.xsede_channel.basicConsume(self._jsonMessage,"carnac.sysinfo",no_ack=False)
        self.connected = True

    def _connectX509(self, host):
        logger.info("connecting to %s" % host)
        ssl_options = {"keyfile": "/home/karnak/cert/serverkey.pem",
                       "certfile": "/home/karnak/cert/servercert.pem",
                       "cert_reqs": ssl.CERT_REQUIRED,
                       "ca_certs": "/home/karnak/cert/cacerts.pem"}

        self.xsede_connection = Connection(host=host,
                                           port=5671,
                                           virtual_host="xsede",
                                           mechanism=X509Mechanism(),
                                           ssl_options=ssl_options,
                                           heartbeat=60,
                                           closed_callback=self._closed)
        self.xsede_channel = self.xsede_connection.channel()
        #queue_name = self.xsede_channel.queueDeclare(queue="carnac.sysinfo",durable=True,exclusive=False)
        #self.xsede_channel.queueBind(queue_name,"glue2.compute","#")
        self.xsede_channel.basicConsume(self._jsonMessage,"carnac.sysinfo",no_ack=False)
        self.connected = True

    def _closed(self, error_msg):
        logger.warn("connection closed: %s",error_msg)
        self.connected = False

    def _xmlMessage(self, consumer_tag, routing_key, exchange, content):
        logger.info("XML compute message for %s" % routing_key)
        system = self._xmlToSystem(routing_key,content)
        self._updateSystem(system)

    def _jsonMessage(self, consumer_tag, routing_key, exchange, content):
        logger.info("JSON compute message for %s" % routing_key)
        system = self._jsonToSystem(routing_key,content)
        self._updateSystem(system)

    def _xmlToSystem(self, system_name, content):
        doc = xml.dom.minidom.parseString(content)

        system = System()
        system.name = system_name
        #system.name = getText(doc,"ResourceID")
        system.time = convertToEpoch(getAttribute(doc,"ComputingService","CreationTime"))

        nodes = 0
        processors = 0
        eeNodes = doc.getElementsByTagName("ExecutionEnvironment")
        for node in eeNodes:
            tiNodes = node.getElementsByTagName("TotalInstances")
            if len(tiNodes) == 0:
                continue
            instances = int(tiNodes[0].firstChild.data)
            cpuNodes = node.getElementsByTagName("LogicalCPUs")
            if len(cpuNodes) == 0:
                continue
            cpus = int(cpuNodes[0].firstChild.data)

            # guess if this is really a compute node
            if instances > 1 or cpus > 32:
                nodes = nodes + instances
                processors = processors + instances * cpus
        if processors > 0:
            system.processors = processors
            system.procs_per_node = processors / nodes

        csNodes = doc.getElementsByTagName("ComputingShare")
        for node in csNodes:
            queue = Queue(system)
            queue.name = getText(node,"Name")
            if queue.name == "":
                continue
            max_wall_time_str = getText(node,"MaxWallTime")
            if max_wall_time_str != "":
                queue.max_wall_time = int(max_wall_time_str)
            max_slots_str = getText(node,"MaxSlotsPerJob")
            if max_slots_str != "":
                queue.max_processors = int(max_slots_str)
            system.queues[queue.name] = queue

        return system

    def _jsonToSystem(self, system_name, content):
        doc = json.loads(content)
        
        system = System()
        system.name = system_name

        nodes = 0
        processors = 0
        for exec_env in doc.get("ExecutionEnvironment",[]):
            if system.time == -1:
                system.time = convertToEpoch(exec_env["CreationTime"])
            # guess if this is really a compute node
            try:
                if exec_env["TotalInstances"] > 1 or exec_env["LogicalCPUs"] > 32:
                    nodes = nodes + exec_env["TotalInstances"]
                    processors = processors + exec_env["TotalInstances"] * exec_env["LogicalCPUs"]
            except KeyError:
                pass
        if processors > 0:
            system.processors = processors
            system.procs_per_node = processors / nodes

        for share in doc.get("ComputingShare",[]):
            queue = Queue(system)
            queue.name = share["Name"]
            queue.max_wall_time = share.get("MaxWallTime",-1)
            queue.max_processors = share.get("MaxSlotsPerJob",-1)
            system.queues[queue.name] = queue

        return system

    def _updateSystem(self, system):
        self._teraGridToXsede(system)
        logger.info("updating system %s",system.name)
        try:
            #logger.debug(system)
            db = Glue2Database()
            db.connect()
            db.updateSystemInfo(system)
            db.close()
        except Exception:
            traceback.print_exc()
            #logger.error(content)

    def _teraGridToXsede(self, system):
        system.name = system.name.replace("teragrid.org","xsede.org")
        for queue in system.queues.values():
            queue.system = system.name

#######################################################################################################################

if __name__ == "__main__":
    daemon = SysInfo()
    daemon.start()
