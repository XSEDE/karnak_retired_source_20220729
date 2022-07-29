
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

logging.config.fileConfig("/home/karnak/karnak/etc/logging.conf")

logger = logging.getLogger("karnak.archive")

#######################################################################################################################

class Archiver(Daemon):
    def __init__(self):
        #Daemon.__init__(self,
        #                pidfile=os.path.join(karnak_path,"var","archive.pid"),
        #                stdout=os.path.join(karnak_path,"var","archive.log"),
        #                stderr=os.path.join(karnak_path,"var","archive.log"))
        self.connection = None
        self.channel = None

    def run(self):
        db = Glue2Database()
        db.connect()
        db.createTables()
        db.close()
        sys.exit(0)

        while True:
            if not self.connection:
                try:
                    self._connect("info1.dyn.xsede.org")
                except:
                    traceback.print_exc()
                    self._connect("info2.dyn.xsede.org")
            self.channel.wait()

    def _connect(self, host):
        logger.info("connecting to %s" % host)
        ssl_options = {"cert_reqs": ssl.CERT_REQUIRED,
                       "ca_certs": "/home/karnak/karnak/etc/cacerts.pem"}
        self.connection = amqp.Connection(host=host+":5671",
                                          userid="carnac",
                                          password="<DELETED>",
                                          virtual_host="xsede",
                                          ssl=ssl_options)
        self.channel = self.connection.channel()
        logger.info("_connect consuming from carnac.archive")

        # testing
        #declare_ok = self.channel.queue_declare()
        #activities_queue = declare_ok.queue
        #self.channel.queue_bind(activities_queue,"glue2.computing_activities","#")
        #self.channel.basic_consume(activities_queue,callback=self._queueMessage)

        # testing
        #declare_ok = self.channel.queue_declare()
        #activity_queue = declare_ok.queue
        #self.channel.queue_bind(activity_queue,"glue2.computing_activity","#")
        #self.channel.basic_consume(activity_queue,callback=self._jobMessage)

        self.channel.basic_consume("carnac.tmp.archive",callback=self._queueMessage)
        self.channel.basic_consume("carnac.tmp.archive.activity",callback=self._jobMessage)

    def _connectX509(self, host):
        logger.info("connecting to %s" % host)
        ssl_options = {"keyfile": "/home/karnak/karnak/etc/serverkey.pem",
                       "certfile": "/home/karnak/karnak/etc/servercert.pem",
                       "cert_reqs": ssl.CERT_REQUIRED,
                       "ca_certs": "/home/karnak/karnak/etc/cacerts.pem"}
        self.connection = amqp.Connection(host=host+":5671",
                                          virtual_host="xsede",
                                          ssl=ssl_options)
        self.channel = self.connection.channel()

        logger.info("_connectSecure consuming from carnac.archive")
        self.channel.basic_consume(self._queueMessage,"carnac.tmp.archive")
        self.channel.basic_consume(self._jobMessage,"carnac.tmp.archive.activity")
        self.connected = True

    # not used
    def _closed(self, error_msg):
        logger.warn("connection closed: %s",error_msg)
        self.connection = None
        self.channel = None

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

    def _jobMessage(self, message):
        routing_key = message.delivery_info["routing_key"]
        content = message.body

        logger.info("processing job message %s",routing_key)
        doc = json.loads(content)
        job = Job()
        try:
            system = ".".join(routing_key.split(".")[2:]) # drop job id and user
            job.fromGlue2Json(system,doc)
        except Exception, e:
            logger.error(str(e))
        else:
            db = Glue2Database()
            db.connect()
            try:
                if db.updateJob(job):
                    db.writeJobQueue(job)
            except Exception:
                logger.error("failed to update job: %s",traceback.format_exc())
            db.close()
        logger.debug("done with job message %s",routing_key)
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
        logger.info("  %d jobs from %s at %s" % (len(state.jobs),state.system,
                                                 datetime.datetime.utcfromtimestamp(state.time).isoformat()))

        last_state = db.readLastQueueState(state.system)
        if last_state is None:
            last_state = QueueState()
        else:
            # in case we are getting older job info from the WS-MDS
            logger.debug("    last update at %d" % last_state.time)
            if state.time < last_state.time:
                logger.info("  time of jobs older than previous data, ignoring jobs")
                return
            if state.time == last_state.time:
                logger.info("  time of jobs is the same as previous data, ignoring jobs")
                return
            last_state.jobs = db.readLastJobs(state.system)

        # generate new jobs and update existing jobs
        for job in state.jobs.values():
            if job == last_state.jobs.get(job.id):
                # no new information, so nothing to do
                logger.debug("no new info for job %s on %s",job.id,job.system)
                continue
            if not db.updateJob(job,last_state.jobs.get(job.id,None)):
                # job wasn't updated in the database (e.g. a job update already triggered a db update)
                continue
            if job.state == Job.PENDING:
                # no experience needs to be generated
                continue
            db.writeJobQueue(job)

        # update jobs that disappeared
        for id in last_state.job_ids:
            if id in state.jobs:
                # we know about this job - it didn't disappear
                continue
            if id not in last_state.jobs:  # sanity check
                logger.error("job %s for %s not in dictionary with %d jobs",id,state.system,len(last_state.jobs))
                continue
            if last_state.jobs[id].state == Job.DONE:
                # job was done in last state, so it didn't disappear
                continue
            job = copy.copy(last_state.jobs[id])
            job.state = Job.DISAPPEARED
            job.time = state.time
            job.end_time = state.time
            logger.debug("job %s on %s disappeared",job.id,job.system)
            db.updateJob(job)
            db.writeJobQueue(job)

        # save job ids in order so that:
        #   * job contexts can be generated for wait time experiences
        #   * historical scheduling simulations can be performed
        #   * the scheduling algorithm can be learned
        logger.debug("  writing queue state")
        try:
            db.writeQueueState(state)
        except Exception:
            # in case archive2.py already wrote it
            logger.error("failed to write queue state: %s",traceback.format_exc())

        logger.debug("  writing last queue state and jobs")
        try:
            db.writeLastQueueState(state)
        except Exception, e:
            logger.error("failed to write last queue state: %s",e)
            traceback.print_exc()
        db.writeLastJobs(state.system,state.jobs.values())

    def _teraGridToXsede(self, state):
        state.system = state.system.replace("teragrid.org","xsede.org")
        for job in state.jobs.values():
            job.system = state.system

#######################################################################################################################

if __name__ == "__main__":
    daemon = Archiver()
    #daemon.start()
    daemon.run()
