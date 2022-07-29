#!/usr/bin/env python

import datetime
import logging
import logging.config
import os
import sys
import time

import MySQLdb

from ipf.daemon import Daemon

from carnac.database import Glue2Database
from carnac.job import *

libexec_path = os.path.split(os.path.abspath(__file__))[0]
carnac_path = os.path.split(libexec_path)[0]

logging.config.fileConfig(os.path.join(carnac_path,"etc","logging.conf"))

logger = logging.getLogger("carnac.generate_karnak")

#######################################################################################################################

class GenerateExperiences(Daemon):
    def __init__(self):
        Daemon.__init__(self,
                        pidfile=os.path.join(carnac_path,"var","generate_karnak.pid"),
                        stdout=os.path.join(carnac_path,"var","generate_karnak.log"),
                        stderr=os.path.join(carnac_path,"var","generate_karnak.log"))

    def run(self):
        while True:
            self.generateExperiences()
            time.sleep(30)

    def generateExperiences(self):
        self.glue_db = Glue2Database()
        self.glue_db.connect()
        self.karnak_db = Glue2Database()
        self.karnak_db.connectKarnak()
        while self._generateExperiences():
            pass
        self.glue_db.close()
        self.glue_db = None
        self.karnak_db.close()
        self.karnak_db = None

    def _generateExperiences(self):
        limit = 100
        logger.info("getting first %d job states in queue..." % limit)
        cursor = self.glue_db.conn.cursor()
        cursor.execute("select * from job_queue_karnak order by time asc limit "+str(limit))
        jobs = []
        for row in cursor:
            job = Job()
            job.fromSql(row,True)
            jobs.append(job)
        cursor.close()


        for job in jobs:
            if job.submit_time <= 0:
                logger.warn("found job for %s on %s submitted at %d - ignoring" % (job.id,job.system,job.submit_time))
                continue
            if job.start_time > 0:
                logger.info("generating wait time experiences for %s on %s" % (job.id,job.system))
                self.generateUnsubmittedWaitTimeExperience(job)
                self.generateSubmittedWaitTimeExperience(job)
            if job.end_time > 0:
                logger.info("generating run time experience for %s on %s" % (job.id,job.system))
                self.generateRunTimeExperience(job)

        cursor = self.glue_db.conn.cursor()
        for job in jobs:
            cursor.execute("delete from job_queue_karnak where system=%s and id=%s and time=%s",
                           (job.system,job.id,job.time))
        cursor.close()

        if len(jobs) == limit:
            return True
        else:
            return False

    def generateRunTimeExperience(self, job):
        if not self._goodRunTimeJob(job):
            return

        if job.state == Job.DISAPPEARED:
            queue_state = self.glue_db.readQueueStateBefore(job.system,job.end_time)
            if queue_state is None or job.end_time - queue_state.time > 10 * 60:
                logger.info("  no run time experience for %s on %s - end time is bad" %
                            (job.id,job.system))
                return

        exp = RunTimeExperience()
        exp.fromJob(job)

        logger.info("run time "+str(exp))
        self.writeRunTimeExperience(exp)
        #self.writeRunTimeExperienceToQueue(exp)

    def _goodRunTimeJob(self, job):
        if job == None:
            logger.warn("  failed to read job")
            return False
        if job.submit_time == -1:
            logger.warn("  submit time of job %s on %s is -1" % (job.id,job.system))
            return False
        if job.start_time == -1:
            logger.warn("  start time of job %s on %s is -1" % (job.id,job.system))
            return False
        if job.end_time == -1:
            logger.warn("  end time of job %s on %s is -1" % (job.id,job.system))
            return False
        return True

    def writeRunTimeExperience(self, exp):
        cursor = self.karnak_db.conn.cursor()
        try:
            cursor.execute("insert into runtime_experiences values ("+exp.toSql()+")")
        except MySQLdb.Error, e:
            logger.warning("failed to insert runtime experience: %s",e.args[1])
        cursor.close()
        
    def writeRunTimeExperienceToQueue(self, exp):
        pass
        
    def generateUnsubmittedWaitTimeExperience(self, job):
        if not self._goodWaitTimeJob(job):
            return
        queue_state = self.glue_db.readQueueStateBefore(job.system,job.submit_time)
        #print("queue state before %f: %s" % (job.getSubmitTime(),queue_state))
        if queue_state == None:
            logger.warn("  didn't find queue state for %s on %s before %d" %
                        (job.id,job.system,job.submit_time))
            return
        if job.submit_time - queue_state.time > 10 * 60:
            logger.info("  no unsubmitted experience for %s on %s - previous queue state is %d mins before" %
                        (job.id,job.system,(job.submit_time-queue_state.time)/60))
            return

        logger.info("  getting information on %d jobs..." % len(queue_state.job_ids))
        queue_state.jobs = self.glue_db.getJobs(queue_state.system,queue_state.job_ids)

        if len(queue_state.jobs) < len(queue_state.job_ids):
            logger.warn("  found %d of %d jobs" % (len(queue_state.jobs),len(queue_state.job_ids)))
            if len(queue_state.jobs) < len(queue_state.job_ids) * 0.95:
                logger.warn("    not generating unsubmitted experience")
                return

        queue_state.jobs.append(job)

        exp = WaitTimeExperience()
        exp.fromJob(job)
        exp.fromQueueState(job.id,queue_state)
        # using the time of job submission may be better than using the time of the queue state just before submission
        exp.time = job.submit_time
        
        logger.info("unsubmitted "+str(exp))
        logger.info("from "+str(job))
        self.writeUnsubmittedExperience(exp)
        #self.writeUnsubmittedExperienceToQueue(exp)

    def _goodWaitTimeJob(self, job):
        if job == None:
            logger.warn("  failed to read job")
            return False
        if job.submit_time == -1:
            logger.warn("  submit time of job %s on %s is -1" % (job.id,job.system))
            return False
        if job.start_time == -1:
            logger.warn("  start time of job %s on %s is -1" % (job.id,job.system))
            return False
        return True

    def writeUnsubmittedExperience(self, exp):
        cursor = self.karnak_db.conn.cursor()
        try:
            cursor.execute("insert into unsubmitted_experiences values ("+exp.toSql()+")")
        except MySQLdb.Error, e:
            logger.warning("failed to insert unsubmitted experience: %s",e.args[1])
        cursor.close()

    def writeUnsubmittedExperienceToQueue(self, exp):
        pass

    def generateSubmittedWaitTimeExperience(self, job):
        if job == None:
            return
        if not self._goodWaitTimeJob(job):
            return

        wait_time = job.start_time - job.end_time
        #print("wait time is %d" % wait_time)
        exp_time = self._generateSubmittedWaitTimeExperience(job,job.submit_time)
        exp_time = self._generateSubmittedWaitTimeExperience(job,max(job.submit_time+wait_time/3,exp_time))
        self._generateSubmittedWaitTimeExperience(job,max(job.submit_time+2*wait_time/3,exp_time))

    def _generateSubmittedWaitTimeExperience(self, job, time):
        queue_state = self.glue_db.readQueueStateAfter(job.system,time)
        #print("queue state after: "+str(queue_state))
        if queue_state == None:
            logger.warn("  didn't find queue state for %s after %d" % (job.system,time))
            return
        if job.id not in queue_state.job_ids:
            logger.warn("  didn't find job %s in queue state for %s at %d" % (job.id,job.system,queue_state.time))
            return
        if not job.pendingAtTime(queue_state.time):
            logger.warn("  job %s on %s is not pending at %s" % (job.id,job.system,
                                                                 datetime.datetime.utcfromtimestamp(queue_state.time)))
            return

        logger.info("  getting information on %d jobs..." % len(queue_state.job_ids))
        queue_state.jobs = self.glue_db.getJobs(queue_state.system,queue_state.job_ids)

        if len(queue_state.jobs) < len(queue_state.job_ids):
            logger.warn("  found %d of %d jobs" % (len(queue_state.jobs),len(queue_state.job_ids)))
            if len(queue_state.jobs) < len(queue_state.job_ids) * 0.95:
                logger.warn("  not generating unsubmitted experience")
                return

        exp = WaitTimeExperience()
        exp.fromJob(job)
        exp.fromQueueState(job.id,queue_state)    # will stop when it hits the job id

        # using the time of job submission may be better than using the time of the queue state just before submission
        if exp.time < job.submit_time:
            exp.time = job.submit_time
            
        logger.info("submitted "+str(exp))
        self.writeSubmittedExperience(exp)
        #self.writeSubmittedExperienceToQueue(exp)

        return exp.time

    def writeSubmittedExperience(self, exp):
        cursor = self.karnak_db.conn.cursor()
        try:
            cursor.execute("insert into submitted_experiences values ("+exp.toSql()+")")
        except MySQLdb.Error, e:
            logger.warning("failed to insert submitted experience: %s",e.args[1])
        cursor.close()

    def writeSubmittedExperienceToQueue(self, exp):
        pass

#######################################################################################################################

class RunTimeExperience(object):
    def __init__(self):
        self.system = None
        self.id = None
        self.name = None
        self.user = None
        self.queue = None
        self.project = None
        self.processors = -1
        self.requestedWallTime = -1

        self.submitTime = -1
        self.startTime = -1
        self.endTime = -1

    def __str__(self):
        expStr = "experience "+self.id+" on "+self.system+":\n"
        if self.name != "":
            expStr = expStr + "  name: "+self.name+"\n"
        expStr = expStr + "  user: "+self.user+"\n"
        expStr = expStr + "  queue: "+self.queue+"\n"
        expStr = expStr + "  project: "+self.project+"\n"
        expStr = expStr + "  processors: "+str(self.processors)+"\n"
        expStr = expStr + "  requested wall time: "+hms(self.requestedWallTime)+"\n"

        expStr = expStr + "  run time: "+hms(self.endTime-self.startTime)+"\n"
        expStr = expStr + "    submit time: "+datetime.datetime.utcfromtimestamp(self.submitTime).isoformat()+"\n"
        expStr = expStr + "    start time: "+datetime.datetime.utcfromtimestamp(self.startTime).isoformat()+"\n"
        expStr = expStr + "    end time: "+datetime.datetime.utcfromtimestamp(self.endTime).isoformat()+"\n"

        return expStr

    def getSqlDef(cls):
        return "system varchar(80), id varchar(80), name text, user text, queue text, project text, " + \
            "processors integer, requestedWallTime integer, " + \
            "submitTime real, startTime real, endTime real";
    getSqlDef = classmethod(getSqlDef)

    def fromExperience(self, exp):
	self.system = exp.system
	self.id = exp.id
	self.name = exp.name
	self.user = exp.user
	self.queue = exp.queue
	self.project = exp.project
	self.processors = exp.processors
	self.requestedWallTime = exp.requestedWallTime

	self.submitTime = exp.submitTime
	self.startTime = exp.startTime
	self.endTime = exp.endTime

    def fromJob(self, job):
	self.system = job.system
	self.id = job.id
	self.name = job.name
	self.user = job.user
	self.queue = job.queue
	self.project = job.project
	self.processors = job.processors
	self.requestedWallTime = job.requested_wall_time

	self.submitTime = job.submit_time
	self.startTime = job.submit_time
	self.endTime = job.end_time

    def fromSql(self, row):
        self.system = row[0]
        self.id = row[1]
        self.name = row[2]
        self.user = row[3]
        self.queue = row[4]
        self.project = row[5]
        self.processors = row[6]
        self.requestedWallTime = row[7]
        self.submitTime = row[8]
        self.startTime = row[9]
        self.endTime = row[10]

    def toSql(self):
        return "'" + self.system + "', " + \
            "'" + self.id + "', " + \
            "'" + self.name + "', " + \
            "'" + self.user + "', " + \
            "'" + self.queue + "', " + \
            "'" + self.project + "', " + \
            str(self.processors) + ", " + \
            str(self.requestedWallTime) + ", " + \
	    str(self.submitTime) + ", " + \
	    str(self.startTime) + ", " + \
	    str(self.endTime)

    def getTime(self):
        return self.endTime

#######################################################################################################################

class WaitTimeExperience(object):

    def __init__(self):
        self.system = None
        self.id = None
        self.name = None
        self.user = None
        self.queue = None
        self.project = None
        self.processors = -1
        self.requestedWallTime = -1

        self.time = -1

        self.count = -1
        self.work = -1
        self.userCount = -1
        self.userWork = -1
        self.jobsRunning = False

        self.simulatedStartTime = -1

        self.startTime = -1

    def __str__(self):
        expStr = "experience for "+self.id+" on "+self.system+":\n"
        if self.name != "":
            expStr = expStr + "  name: "+self.name+"\n"
        expStr = expStr + "  user: "+self.user+"\n"
        expStr = expStr + "  queue: "+self.queue+"\n"
        expStr = expStr + "  project: "+self.project+"\n"
        expStr = expStr + "  processors: "+str(self.processors)+"\n"
        expStr = expStr + "  requested wall time: "+hms(self.requestedWallTime)+"\n"
        expStr = expStr + "  time: "+datetime.datetime.utcfromtimestamp(self.time).isoformat()+"\n"

        expStr = expStr + "  count: "+str(self.count)+"\n"
        expStr = expStr + "  work: "+str(self.work)+"\n"
        expStr = expStr + "  userCount: "+str(self.userCount)+"\n"
        expStr = expStr + "  userWork: "+str(self.userWork)+"\n"
        expStr = expStr + "  jobs running: "+str(self.jobsRunning)+"\n"

        expStr = expStr + "  simulated start time: "
        if self.simulatedStartTime == -1:
            expStr = expStr + "(none)\n"
        else:
            expStr = expStr + datetime.datetime.utcfromtimestamp(self.simulatedStartTime).isoformat()+"\n"

        expStr = expStr + "  start time: "
        if self.startTime == -1:
            expStr = expStr + "(none)\n"
        else:
            expStr = expStr + datetime.datetime.utcfromtimestamp(self.startTime).isoformat()+"\n"

        return expStr


    def getSqlDef(cls):
        return "system varchar(80), id varchar(80), name text, user text, queue text, project text, " + \
            "processors integer, requestedWallTime integer, time real, " + \
            "count integer, work real, userCount integer, userWork real, jobsRunning bit(1), " + \
	    "simulatedStartTime real, startTime real"
    getSqlDef = classmethod(getSqlDef)

    def fromExperience(self, exp):
	self.system = exp.system
	self.id = exp.id
	self.name = exp.name
	self.user = exp.user
	self.queue = exp.queue
	self.project = exp.project
	self.processors = exp.processors
	self.requestedWallTime = exp.requestedWallTime

        self.time = -1

        self.count = -1
        self.work = -1
        self.userCount = -1
        self.userWork = -1
        self.jobsRunning = False

        self.simulatedStartTime = -1

        self.startTime = -1

    def fromJob(self, job):
        self.id = job.id
        self.name = job.name
        self.user = job.user
        self.queue = job.queue
        self.project = job.project
        self.processors = job.processors
        self.requestedWallTime = job.requested_wall_time
        self.startTime = job.start_time

    def fromQueueState(self, jobId, queueState):
	self.system = queueState.system

        self.time = queueState.time

        # jobsRunning can now only be false only when there are jobs queued
        if len(queueState.jobs) == 0:
            self.jobsRunning = True
        else:
            self.jobsRunning = False
            for job in queueState.jobs:
                if job.runningAtTime(queueState.time):
                    self.jobsRunning = True
                    break

        self.count = 0
        self.work = 0
        self.userCount = 0
        self.userWork = 0
        for job in queueState.jobs:
            if job.id == jobId:
                return
            self.count = self.count + 1
            if job.runningAtTime(queueState.time):
                runningTime = queueState.time - job.start_time
                if runningTime > job.requested_wall_time:
                    runningTime = job.requested_wall_time
            else:
                runningTime = 0
            self.work = self.work + job.processors * (job.requested_wall_time - runningTime)
            if job.user == self.user:
                self.userCount = self.userCount + 1
                self.userWork = self.userWork = job.processors * (job.requested_wall_time - runningTime)
        raise UnknownJobError(queueState.system,jobId)

    def fromSql(self, row):
        self.system = row[0]
        self.id = row[1]
        self.name = row[2]
        self.user = row[3]
        self.queue = row[4]
        self.project = row[5]
        self.processors = row[6]
        self.requestedWallTime = row[7]
        self.time = row[8]

        self.count = row[9]
        self.work = row[10]
        self.userCount = row[11]
        self.userWork = row[12]
        if row[13] == 0:
            self.jobsRunning = False
        else:
            self.jobsRunning = True

        self.simulatedStartTime = row[14]

        self.startTime = row[15]


    def toSql(self):
        sqlStr = "'" + self.system + "', " + \
            "'" + self.id + "', " + \
            "'" + self.name + "', " + \
            "'" + self.user + "', " + \
            "'" + self.queue + "', " + \
            "'" + self.project + "', " + \
            str(self.processors) + ", " + \
            str(self.requestedWallTime) + ", " + \
	    str(self.time) + ", " + \
	    str(self.count) + ", " + \
	    str(self.work) + ", " + \
	    str(self.userCount) + ", " + \
	    str(self.userWork) + ", "
        if self.jobsRunning:
            sqlStr = sqlStr + "1, "
        else:
            sqlStr = sqlStr + "0, "
	sqlStr = sqlStr + str(self.simulatedStartTime) + ", " + str(self.startTime)

	return sqlStr;

#######################################################################################################################

if __name__ == "__main__":
    generator = GenerateExperiences()
    generator.start()
    #generator.run()
