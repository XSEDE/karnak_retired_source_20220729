
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

import amqp

from karnak.daemon import Daemon
from karnak.database import Glue2Database
from karnak.job import *

karnak_path = "/home/karnak/karnak"
logging.config.fileConfig(os.path.join(karnak_path,"etc","logging.conf"))

logger = logging.getLogger("karnak.archive")

#######################################################################################################################

class QueueOrder(object):
    def __init__(self):
        self.connection = None
        self.channel = None

    def run(self):
        while True:
            if not self.connection:
                try:
                    self._connect("infopub.xsede.org")
                except:
                    traceback.print_exc()
                    self._connect("infopub-alt.xsede.org")
            self.channel.wait()

    def _connect(self, host):
        logger.info("connecting to %s" % host)
        ssl_options = {"cert_reqs": ssl.CERT_REQUIRED,
                       "ca_certs": "/home/karnak/karnak/etc/cacerts.pem"}
        self.connection = amqp.Connection(host=host+":5671",
                                          userid="karnak",
                                          password="<DELETED>",
                                          virtual_host="xsede",
                                          ssl=ssl_options)
        self.channel = self.connection.channel()

        logger.info("_connect consuming")

        declare_ok = self.channel.queue_declare()
        activities_queue = declare_ok.queue
        self.channel.queue_bind(activities_queue,"glue2.computing_activities","#")
        self.channel.basic_consume(activities_queue,callback=self._queueMessage)

    def _queueMessage(self, message):
        routing_key = message.delivery_info["routing_key"]
        content = message.body

        logger.info("queue message for %s" % routing_key)
        # if there are no jobs, there isn't any metadata (like at what time there were no jobs)
        #   include the ComputingService to give context?
        db = Glue2Database()
        db.connect()
        try:
            state = self._toQueueState(routing_key,content)
            self._handleQueueState(state,db)
        except Exception:
            logger.error(traceback.format_exc())
        db.close()
        logger.debug("done with queue message for %s" % routing_key)
        self.channel.basic_ack(message.delivery_tag)

    def _toQueueState(self, system, content):
        state = QueueState()
        state.system = system
        doc = json.loads(content)
        for act_doc in doc.get("ComputingActivity",[]):
            job = Job()
            try:
                job.fromGlue2Json(system,act_doc)
            except Exception, e:
                logger.error(str(e))
            else:
                state.job_ids.append(job.id)
                state.jobs[job.id] = job

        if len(state.jobs) > 0:
            state.time = state.jobs.values()[0].time # assume all jobs in a snapshot have the same time
        else:
            logger.warning("no jobs in JSON message from %s -  can't compute current time, assuming current time",
                           system)
            state.time = time.time()
            
        return state

    def _handleQueueState(self, state, db):
        self._teraGridToXsede(state)
        logger.info("  %d jobs from %s at %s" % (len(state.jobs),state.system,state.time.isoformat()))

        last_submit_time = None
        num_pending = 0
        for job_id in state.job_ids:
            job = state.jobs[job_id]
            if job.submit_time is None:
                logger.info("    job has None submit time")
                return
            if job.state != Job.PENDING:
                continue
            num_pending += 1
            #print("      %s" % job.submit_time)
            if last_submit_time is not None:
                if job.submit_time < last_submit_time:
                    logger.info("    not in submit time order")
                    return
            last_submit_time = job.submit_time
        logger.info("    %d pending jobs in submit time order" % num_pending)

    def _teraGridToXsede(self, state):
        state.system = state.system.replace("teragrid.org","xsede.org")
        for job in state.jobs.values():
            job.system = state.system

#######################################################################################################################

if __name__ == "__main__":
    qo = QueueOrder()
    qo.run()
