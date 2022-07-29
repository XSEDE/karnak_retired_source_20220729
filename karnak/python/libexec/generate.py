#!/usr/bin/env python

###############################################################################
#   Copyright 2015 The University of Texas at Austin                          #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at                                   #
#                                                                             #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
###############################################################################

import datetime
import logging
import logging.config
import os
import sys
import time

import MySQLdb

from karnak.daemon import Daemon
from karnak.database import *
from karnak.job import *

karnak_path = "/home/karnak/karnak"
logging.config.fileConfig(os.path.join(karnak_path,"etc","logging.conf"))

logger = logging.getLogger("karnak.generate")

#######################################################################################################################

class GenerateExperiences(Daemon):
    def __init__(self):
        Daemon.__init__(self,
                        pidfile=os.path.join(karnak_path,"var","generate.pid"),
                        stdout=os.path.join(karnak_path,"var","generate.log"),
                        stderr=os.path.join(karnak_path,"var","generate.log"))
        self.glue_db = None
        self.karnak_db = None
        self.oldest_day = None

    def run(self):
        #db = KarnakDatabase()
        #db.connect()
        #db.createTables()
        #db.close()
        #sys.exit(0)

        while True:
            self.generateExperiences()
            time.sleep(60)

    def generateExperiences(self):
        self.glue_db = Glue2Database()
        self.glue_db.connect()
        self.karnak_db = KarnakDatabase()
        self.karnak_db.connect()

        while self._generateExperiences():
            pass
        self.glue_db.close()
        self.glue_db = None
        self.karnak_db.close()
        self.karnak_db = None

    def _find_oldest_day(self):
        if self.oldest_day is None:
            cursor = self.glue_db.conn.cursor()
            cursor.execute("select * from started_jobs where hasExperience=false")
            for row in cursor:
                this_day = row[2].strftime('%Y-%m-%d')
                if self.oldest_day is None:
                    self.oldest_day = this_day
                elif this_day < self.oldest_day:
                    self.oldest_day = this_day
            cursor.close()
            if self.oldest_day is None:
                logger.info("found NO oldest_day")
            else:
                logger.info("found oldest_day="+self.oldest_day)

    def _generateExperiences(self):
        self._find_oldest_day()
        if self.oldest_day is None:
            return False
        limit = 200
        cursor = self.glue_db.conn.cursor()
        logger.info("getting first %d started jobs without experiences from %s" % (limit, self.oldest_day))
        cursor.execute("select * from started_jobs where hasExperience=false and DATE_FORMAT(submitTime, '%Y-%m-%d')='"+self.oldest_day+"' limit "+str(limit))
        jobs = {}
        for row in cursor:
            jobs[(row[0],row[1],row[2])] = None
        cursor.close()

        for (system,id,submit_time) in jobs:
            job = self.glue_db.readJob(system,id,submit_time)
            if job is None:
                jobs_with_id = self.glue_db.readJobsWithId(system,id)
                if len(jobs_with_id) == 1:
                    logger.info("found job %s on %s at different submit time" % (id,system))
                    job = jobs_with_id[0]
            if job is not None:
                jobs[(system,id,submit_time)] = job

        for (system,id,submit_time) in jobs:
            job = jobs[(system,id,submit_time)]
            try:
                if job is None:
                    pass
                if job.submit_time is None:
                    logger.warn("found job %s on %s with no submit time - ignoring" % (job.id,job.system))
                elif job.start_time is not None:
                    logger.info("generating wait time experiences for %s on %s" % (job.id,job.system))
                    self.generateUnsubmittedWaitTimeExperience(job)
                    self.generateSubmittedWaitTimeExperience(job)
            except:
                logger.error(traceback.format_exc())
            else:
                # mark the job as done unless an exception while writing experiences
                try:
                    cursor = self.glue_db.conn.cursor()
                    cursor.execute("update started_jobs set hasExperience=TRUE where "+
                                   "system=%s and id=%s and submitTime=%s",
                                   (system,id,submit_time))
                    cursor.close()
                except:
                    logger.error(traceback.format_exc())

        if len(jobs) > 0:
            logger.info("generated for %d jobs" % len(jobs))
        if len(jobs) == limit:
            return True
        else:
            self.oldest_day = None
            return False
        
    def generateUnsubmittedWaitTimeExperience(self, job):
        if not self._goodWaitTimeJob(job):
            return
        queue_state = self.glue_db.readQueueStateBefore(job.system,job.submit_time)
        #print("queue state before %f: %s" % (job.getSubmitTime(),queue_state))
        if queue_state == None:
            logger.warn("  didn't find queue state for %s on %s before %s" %
                        (job.id,job.system,job.submit_time.isoformat()))
            return
        if job.submit_time - queue_state.time > datetime.timedelta(minutes=10):
            logger.info("  no unsubmitted experience for %s on %s - previous queue state is %d mins before" %
                        (job.id,job.system,(job.submit_time-queue_state.time).total_seconds()/60))
            return

        logger.debug("  getting information on %d jobs..." % len(queue_state.job_ids))
        #for j in self.glue_db.readJobs(queue_state.system,queue_state.job_ids):
        for j in self.glue_db.getJobs(queue_state.system,queue_state.job_ids):
            queue_state.jobs[j.id] = j

        if len(queue_state.jobs) < len(queue_state.job_ids):
            logger.warn("  found %d of %d jobs" % (len(queue_state.jobs),len(queue_state.job_ids)))
            if len(queue_state.jobs) < len(queue_state.job_ids) * 0.95:
                logger.warn("    not generating unsubmitted experience")
                return

        queue_state.job_ids.append(job.id)
        queue_state.jobs[job.id] = job

        exp = WaitTimeExperience()
        exp.fromJobAndQueueState(job,queue_state)
        # using the time of job submission may be better than using the time of the queue state just before submission
        exp.time = job.submit_time
        
        logger.debug("unsubmitted "+str(exp))
        self.karnak_db.writeUnsubmittedExperience(exp)

    def _goodWaitTimeJob(self, job):
        if job == None:
            logger.warn("  failed to read job")
            return False
        if job.submit_time is None:
            logger.warn("  no submit time of job %s on %s" % (job.id,job.system))
            return False
        if job.start_time is None:
            logger.warn("  no start time of job %s on %s" % (job.id,job.system))
            return False
        if job.processors is None:
            logger.warn("  no processors for job %s on %s" % (job.id,job.system))
            return False
        if job.requested_wall_time is None:
            logger.warn("  no requested wall time for job %s on %s" % (job.id,job.system))
            return False
        return True

    def generateSubmittedWaitTimeExperience(self, job):
        if job == None:
            return
        if not self._goodWaitTimeJob(job):
            return

        wait_time = job.start_time - job.submit_time
        exp_time = self._generateSubmittedWaitTimeExperience(job,job.submit_time)
        if exp_time is not None:
            exp_time = self._generateSubmittedWaitTimeExperience(job,max(job.submit_time+wait_time/3,exp_time))
        if exp_time is not None:
            self._generateSubmittedWaitTimeExperience(job,max(job.submit_time+2*wait_time/3,exp_time))

    def _generateSubmittedWaitTimeExperience(self, job, time):
        queue_state = self.glue_db.readQueueStateAfter(job.system,time)
        #print("queue state after: "+str(queue_state))
        if queue_state == None:
            logger.warn("  didn't find queue state for %s after %s" % (job.system,time.isoformat()))
            return None
        if job.id not in queue_state.job_ids:
            logger.warn("  didn't find job %s in queue state for %s at %s" %
                        (job.id,job.system,queue_state.time.isoformat()))
            return None
        if not job.pendingAtTime(queue_state.time):
            logger.warn("  job %s on %s is not pending at %s" %
                        (job.id,job.system,queue_state.time.isoformat()))
            return None

        logger.debug("  getting information on %d jobs..." % len(queue_state.job_ids))
        #for j in self.glue_db.readJobs(queue_state.system,queue_state.job_ids):
        for j in self.glue_db.getJobs(queue_state.system,queue_state.job_ids):
            queue_state.jobs[j.id] = j

        if len(queue_state.jobs) < len(queue_state.job_ids):
            logger.warn("  found %d of %d jobs" % (len(queue_state.jobs),len(queue_state.job_ids)))
            if len(queue_state.jobs) < len(queue_state.job_ids) * 0.95:
                logger.warn("  not generating unsubmitted experience")
                return None

        exp = WaitTimeExperience()
        exp.fromJobAndQueueState(job,queue_state) # will stop when it hits the job id

        # using the time of job submission may be better than using the time of the queue state just before submission
        if exp.time < job.submit_time:
            exp.time = job.submit_time
            
        logger.debug("submitted "+str(exp))
        self.karnak_db.writeSubmittedExperience(exp)

        return exp.time


#######################################################################################################################

if __name__ == "__main__":
    generator = GenerateExperiences()
    generator.start()
    #generator.run()
