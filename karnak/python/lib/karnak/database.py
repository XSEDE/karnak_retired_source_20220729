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

import logging
import time
import traceback

import MySQLdb
import MySQLdb.cursors

from experience import *
from job import *

logger = logging.getLogger(__name__)

def hms(seconds):
    hours = int(math.floor(seconds / (60*60)))
    seconds = seconds - hours * (60*60)
    minutes = int(math.floor(seconds / 60))
    seconds = seconds - minutes * 60;
    return "%02d:%02d:%02d" % (hours,minutes,seconds)

#######################################################################################################################

CURRENT_AGE_SECS = 15 * 60

class Glue2Database(object):
    def __init__(self, cache_jobs=False):
        self.conn = None

        # job caching is safe for the one process writing jobs (archive.py)
        self._cache_jobs = cache_jobs
        self._cache_size_limit = 50000
        self._job_cache = {}

        self._read_only_job_cache = {}   # for generate.py

    def _putCachedJob(self, job):
        if not self._cache_jobs:
            return
        if len(self._job_cache) >= self._cache_size_limit:
            self._job_cache = {}
        self._job_cache[(job.system,job.id)] = job

    def _getCachedJob(self, system, id):
        try:
            return self._job_cache[(system,id)]
        except KeyError:
            return None
    
    def connect(self):
        self.conn = MySQLdb.connect(user="karnak",db="glue2",
                                    cursorclass=MySQLdb.cursors.SSCursor)
        self.conn.autocommit(True)
        
    def close(self):
        self.conn.close()
        self.conn = None

    def createTables(self):
        cursor = self.conn.cursor()

        # archive.py saves all jobs here (how slow will this be to update?)
        cursor.execute("create table if not exists jobs ("+Job.getSqlDef()+", primary key (system,id,submitTime))")

        # generate.py uses to determine what experiences to generate
        cursor.execute("create table if not exists started_jobs (system varchar(80) NOT NULL, id varchar(80) NOT NULL, submitTime datetime NOT NULL, hasExperience boolean DEFAULT false, primary key (system,id,submitTime))")
        cursor.execute("alter table started_jobs add index hasExperience using hash (hasExperience)")

        # used by generate.py
        cursor.execute("create table if not exists queue_states ("+QueueState.getSqlDef()+", "+ \
                       "primary key (system,time))")

        # used by the web interface
        cursor.execute("create table if not exists last_jobs ("+Job.getSqlDef()+", primary key (system,id))")
        cursor.execute("create table if not exists last_queue_states ("+QueueState.getSqlDef()+", "+ \
                       "primary key (system))")

        # used by the web interface
        cursor.execute("create table if not exists system_info ("+System.getSqlDef()+", primary key (system))")
        cursor.execute("create table if not exists queue_info ("+Queue.getSqlDef()+", primary key (system,queue))")

        cursor.close()

    ###################################################################################################################

    # allow caller to provide last_job, if they have it
    def updateJob(self, job, last_job=None, table="jobs"):
        self._putCachedJob(job)
        if last_job is None:
            cursor = self.conn.cursor()
            cursor.execute("select * from "+table+" where system='"+job.system+"' and id='"+job.id+"'")
            row = cursor.fetchone()
            if row is not None:
                last_job = Job()
                last_job.fromSql(row)
            cursor.close()
        if last_job is None:
            logger.debug("insert into "+table+" values ("+job.toSql()+")")
            cursor = self.conn.cursor()
            cursor.execute("insert into "+table+" values ("+job.toSql()+")")
            cursor.close()
            return True
        update = job.toSqlUpdate(last_job)
        if update is not None:
            #logger.debug("existing %s" % last_job)
            #logger.debug("new %s" % job)
            logger.debug("update "+table+" set "+update+" where system=%s and id=%s and submitTime=%s" %
                         (job.system,job.id,job.submit_time))
            cursor = self.conn.cursor()
            cursor.execute("update "+table+" set "+update+" where system=%s and id=%s and submitTime=%s",
                           (job.system,job.id,job.submit_time))
            cursor.close()
            return True
        else:
            logger.debug("nothing to update for job %s on %s",job.id,job.system)
            return False

    # shortcut when we know the job isn't already in the database
    def writeJob(self, job, table="jobs"):
        self._putCachedJob(job)
        #logger.debug("insert into "+table+" values ("+job.toSql()+")")
        cursor = self.conn.cursor()
        cursor.execute("insert into "+table+" values ("+job.toSql()+")")
        cursor.close()

    ###################################################################################################################

    # used by generate.py
    def getJobs(self, system, ids):
        jobs = []
        for id in ids:
            job = self.getJob(system,id)
            if job is not None:
                jobs.append(job)
        return jobs

    def getJob(self, system, id, submit_time = None):
        if (system,id) in self._read_only_job_cache:
            return self._read_only_job_cache[(system,id)]
        job = self.readJob(system,id)
        # only cache jobs that we have all of the information for
        if job is not None and job.end_time is not None:
            if len(self._read_only_job_cache) >= self._cache_size_limit:
                self._read_only_job_cache = {}
            self._read_only_job_cache[(system,id)] = job
        return job
        
    def readJobs(self, system, ids):
        jobs = []
        for id in ids:
            job = self.readJob(system,id)
            if job is not None:
                jobs.append(job)
        return jobs

    def readJob(self, system, id, submit_time = None):
        job = self._getCachedJob(system,id)
        if job is not None:
            return job
        if submit_time is None:
            job = self.readMostRecentJob(system,id)
        else:
            job = self.readJobAtSubmitTime(system,id,submit_time)
        if job is not None:
            self._putCachedJob(job)
        return job

    def readJobAtSubmitTime(self, system, id, submit_time):
        cursor = self.conn.cursor()
        cursor.execute("select * from jobs where system=%s and id=%s and submitTime=%s",(system,id,submit_time))
        row = cursor.fetchone()
        if row == None:
            logger.warn("couldn't read job %s on %s at %s" % (id,system,submit_time.isoformat()))
            cursor.close()
            return None
        job = Job()
        job.fromSql(row)
        cursor.close()
        return job

    def readMostRecentJob(self, system, id):
        jobs = self.readJobsWithId(system,id)
        if len(jobs) > 0:
            return jobs[0]
        else:
            return None

    def readJobsWithId(self, system, id):
        cursor = self.conn.cursor()
        cursor.execute("select * from jobs where system=%s and id=%s order by submitTime desc",(system,id))
        jobs = []
        for row in cursor:
            job = Job()
            job.fromSql(row)
            jobs.append(job)
        cursor.close()
        return jobs

    ###################################################################################################################

    def writeStartedJob(self, job):
        cursor = self.conn.cursor()
        try:
            cursor.execute("insert into started_jobs values (%s,%s,%s,FALSE)",
                           (job.system,job.id,job.submit_time))
        except MySQLdb.IntegrityError:
            # attempting to create a duplicate entry is possible and can be ignored
            pass
        except:
            logger.error("failed to add to experience queue: %s",traceback.format_exc())
        cursor.close()

    ###################################################################################################################

    def writeQueueState(self, state):
        cursor = self.conn.cursor()
        try:
            cursor.execute("insert into queue_states values ("+state.toSql()+")")
        except MySQLdb.IntegrityError:
            logger.error("failed insert queue state: %s",traceback.format_exc())
        except:
            logger.error("failed insert queue state: %s",traceback.format_exc())
        cursor.close()

    def readQueueStateBefore(self, system, time):
        state = None
        cursor = self.conn.cursor()
        cursor.execute("select * from queue_states where system=%s and time<%s order by time desc limit 1",
                       (system,time))
        row = cursor.fetchone()
        if row == None:
            cursor.close()
            return None
        state = QueueState()
        state.fromSql(row)
        cursor.close()
        return state

    def readQueueStateAfter(self, system, time):
        state = None
        cursor = self.conn.cursor()
        cursor.execute("select * from queue_states where system=%s and time>%s order by time asc limit 1",
                       (system,time))
        row = cursor.fetchone()
        if row == None:
            cursor.close()
            return None
        state = QueueState()
        state.fromSql(row)
        cursor.close()
        return state

    ###################################################################################################################

    def writeLastQueueState(self, state):
        cursor = self.conn.cursor()
        cursor.execute("delete from last_queue_states where system=%s",(state.system))
        cursor.execute("insert into last_queue_states values ("+state.toSql()+")")
        cursor.close()

    def readLastQueueState(self, system):
        cursor = self.conn.cursor()
        cursor.execute("select * from last_queue_states where system=%s",(system))
        row = cursor.fetchone()
        if row is None:
            cursor.close()
            return None
        state = QueueState()
        state.fromSql(row)
        cursor.close()
        return state

    def writeLastJobs(self, system, jobs):
        cursor = self.conn.cursor()
        cursor.execute("delete from last_jobs where system=%s",(system))
        for job in jobs:
            try:
                cursor.execute("insert into last_jobs values ("+job.toSql()+")")
            except:
                # write other jobs if one fails
                logger.error(traceback.format_exc())
        cursor.close()

    def updateLastJobs(self, system, jobs):
        job_map = {}
        for job in jobs:
            job_map[job.id] = job
        cur_jobs = self.readLastJobs(system)
        cursor = self.conn.cursor()
        for job in cur_jobs.values():
            if job.id not in job_map:
                cursor.execute("delete from last_jobs where system=%s and id=%s",(system,job.id))
        cursor.close()

        for job in jobs:
            try:
                self.updateJob(job,cur_jobs.get(job.id,None),"last_jobs")
            except:
                # write other jobs if one fails
                logger.error(traceback.format_exc())

    def readLastJobs(self, system):
        jobs = {}
        cursor = self.conn.cursor()
        cursor.execute("select * from last_jobs where system=%s",(system))
        for row in cursor:
            job = Job()
            job.fromSql(row)
            jobs[job.id] = job
        cursor.close()
        return jobs

    ###################################################################################################################

    def updateSystemInfo(self, system):
        cursor = self.conn.cursor()
        cursor.execute("delete from system_info where system=%s",(system.name))
        cursor.execute("insert into system_info values ("+system.toSql()+")")
        cursor.execute("delete from queue_info where system=%s",(system.name))
        for queue in system.queues.values():
            cursor.execute("insert into queue_info values ("+queue.toSql()+")")
        cursor.close()

    def readAllQueueNames(self, system):
        queues = []
        cursor = self.conn.cursor()
        cursor.execute("select distinct(name) from queue_info where system=%s",(system))
        for row in cursor:
            queues.append(row[0])
        cursor.execute("select distinct(queue) from jobs where system=%s",(system))
        for row in cursor:
            if row[0] not in queues:
                queues.append(row[0])
        cursor.close()
        return queues.sort()

#######################################################################################################################

class KarnakDatabase(object):
    def __init__(self):
        self.conn = None

        self.job_cache = {}
    
    def connect(self):
        self.conn = MySQLdb.connect(user="karnak",db="karnak",
                                    cursorclass=MySQLdb.cursors.SSCursor)
        self.conn.autocommit(True)
        
    def close(self):
        self.conn.close()
        self.conn = None

    def createTables(self):
        cursor = self.conn.cursor()

        cursor.execute("create table if not exists unsubmitted_experiences ("+WaitTimeExperience.getSqlDef()+", primary key (system,id,time))")

        cursor.execute("create table if not exists submitted_experiences ("+WaitTimeExperience.getSqlDef()+", primary key (system,id,time))")

        cursor.close()


    def writeUnsubmittedExperience(self, exp):
        cursor = self.conn.cursor()
        try:
            cursor.execute("insert into unsubmitted_experiences values ("+exp.toSql()+")")
        except MySQLdb.Error, e:
            logger.error("failed to insert unsubmitted experience: %s",e.args[1])
        cursor.close()


    def writeSubmittedExperience(self, exp):
        cursor = self.conn.cursor()
        try:
            cursor.execute("insert into submitted_experiences values ("+exp.toSql()+")")
        except MySQLdb.Error, e:
            logger.error("failed to insert submitted experience: %s",e.args[1])
            logger.error("insert into submitted_experiences values ("+exp.toSql()+")")
        cursor.close()

    ###################################################################################################################
