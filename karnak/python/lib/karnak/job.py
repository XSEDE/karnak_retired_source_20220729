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

import calendar
import datetime
import json
import logging

from error import *
from util import *

logger = logging.getLogger(__name__)

##############################################################################################################

class Job(object):

    # simplified queue states (different from previous version)
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    DISAPPEARED = "disappeared"

    def __init__(self):
        self.system = None
        self.id = None
        self.state = None
        self.name = None
        self.user = None
        self.queue = None
        self.project = None
        self.processors = None
        self.requested_wall_time = None  # seconds

        self.time = None               # times are datetime
        self.submit_time = None
        self.start_time = None
        self.end_time = None

        self._warned_requested_wall_time = False

    def __eq__(self, other):
        if other is None:
            return False
        if self.system != other.system:
            return False
        if self.id != other.id:
            return False
        if self.state != other.state:
            return False
        if self.name != other.name:
            return False
        if self.user != other.user:
            return False
        if self.queue != other.queue:
            return False
        if self.project != other.project:
            return False
        if self.processors != other.processors:
            return False
        if self.requested_wall_time != other.requested_wall_time:
            return False
        if self.time != other.time:
            return False
        if self.submit_time != other.submit_time:
            return False
        if self.start_time != other.start_time:
            return False
        if self.end_time != other.end_time:
            return False
        return True
    
    def __str__(self):
        job_str = "job %s on %s:\n" % (self.id,self.system)
        job_str = job_str + "  state: %s\n" % self.state
        if self.name == None:
            job_str = job_str + "  name: %s\n" % self.name
        if self.user is not None:
            job_str = job_str + "  user: %s\n" % self.user
        if self.queue is not None:
            job_str = job_str + "  queue: %s\n" % self.queue
        if self.project is not None:
            job_str = job_str + "  project: %s\n" % self.project
        if self.processors is not None:
            job_str = job_str + "  processors: %d\n" % self.processors
        job_str = job_str + "  requested wall time: %s\n" % hms(self.requested_wall_time)

        if self.time is not None:
            job_str = job_str + "  time: %s\n" % self.time.isoformat()

        if self.submit_time is None:
            job_str = job_str + "  submit time: (none)\n"
        else:
            job_str = job_str + "  submit time: %s\n" % self.submit_time.isoformat()
        if self.start_time is None:
            job_str = job_str + "  start time: (none)\n"
        else:
            job_str = job_str + "  start time: %s\n" % self.start_time.isoformat()
        if self.end_time is None:
            job_str = job_str + "  end time: (none)\n"
        else:
            job_str = job_str + "  end time: %s\n" % self.end_time.isoformat()

        return job_str

    def pendingAtTime(self, time):
        if self.submit_time is None:
            return False
        if self.start_time is None:
            return False
        if time >= self.submit_time and time < self.start_time:
            return True
        return False

    def runningAtTime(self, time):
        if self.start_time is None:
            return False
        if self.end_time is None:
            return False
        if time >= self.start_time and time <= self.end_time:
            return True
        return False

    def work(self):
        if self.requested_wall_time is None:
            if not self._warned_requested_wall_time:
                logger.warn("requested wall time is None for job %s on %s" % (self.id,self.system))
                self._warned_requested_wall_time = True
            return 0
        return self.processors * self.requested_wall_time

    def remainingWork(self, time):
        if self.requested_wall_time is None:
            logger.warn("requested wall time is None for job %s on %s" % (self.id,self.system))
            return 0
        if self.start_time is None:
            return self.work()
        if self.start_time >= time:
            return self.work()
        if self.end_time is not None and self.end_time <= time:
            return 0
        remaining_time = self.requested_wall_time - (time - self.start_time).total_seconds()
        if remaining_time <= 0:
            return 0
        return self.processors * remaining_time

    def getSqlDef(cls, include_time=False):
        sql_str = "system varchar(80) NOT NULL, " + \
                  "id varchar(80) NOT NULL, " + \
                  "state varchar(40), " + \
                  "name text, " + \
                  "user text, " + \
                  "queue text, " + \
                  "project text, " + \
                  "processors integer, " + \
                  "requestedWallTime integer, " + \
                  "submitTime datetime NOT NULL, " + \
                  "startTime datetime, " + \
                  "endTime datetime"
        if include_time:
            sql_str += ", time datetime"
        return sql_str
    getSqlDef = classmethod(getSqlDef)

    def toSql(self, include_time=False):
        sql_str = "'%s'" % self.system
        if self.id is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.id
        if self.state is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.state
        if self.name is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.name
        if self.user is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.user
        if self.queue is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.queue
        if self.project is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.project
        if self.processors is None:
            sql_str += ", NULL"
        else:
            sql_str += ", %d" % self.processors
        if self.requested_wall_time is None:
            sql_str += ", NULL"
        else:
            sql_str += ", %d" % self.requested_wall_time
        if self.submit_time is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.submit_time.strftime("%Y-%m-%d %H:%M:%S")
        if self.start_time is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        if self.end_time is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        if include_time:
            if self.time is None:
                sql_str += ", NULL"
            else:
                sql_str += ", '%s'" % self.time.strftime("%Y-%m-%d %H:%M:%S")
        return sql_str

    def fromSql(self, row, include_time=False):
        self.system = row[0]
        self.id = row[1]
        self.state = row[2]
        self.name = row[3]
        self.user = row[4]
        self.queue = row[5]
        self.project = row[6]
        self.processors = row[7]
        self.requested_wall_time = row[8]
        self.submit_time = row[9]
        self.start_time = row[10]
        self.end_time = row[11]
        if include_time:
            self.time = row[12]

    def fromOrigSql(self, row, include_time=False):
        self.system = row[0]
        self.id = row[1]
        self.state = row[2]
        self.name = row[3]
        self.user = row[4]
        self.queue = row[5]
        self.project = row[6]
        self.processors = row[7]
        self.requested_wall_time = row[8]
        # epoch
        self.submit_time = epochToNaiveDateTime(row[9])
        self.start_time = epochToNaiveDateTime(row[10])
        self.end_time = epochToNaiveDateTime(row[11])
        if include_time:
            self.time = epochToNaiveDateTime(row[12])

    # job has already known information, but it could be newer
    def toSqlUpdate(self, job, include_time=False):
        updates = []
        if self.system != job.system:
            raise KarnakError("can't update a job from a different system!")
        if self.id != job.id:
            raise KarnakError("can't update a job with a different id!")

        if self.state != job.state:
            if self.state == Job.DONE or job.state == Job.DONE:
                state = Job.DONE
            elif self.state == Job.DISAPPEARED or job.state == Job.DISAPPEARED:
                state = Job.DISAPPEARED
            elif self.state == Job.RUNNING or job.state == Job.RUNNING:
                state = Job.RUNNING
            elif self.state == Job.PENDING or job.state == Job.PENDING:
                state = Job.PENDING
            if state != job.state:
                updates.append("state='%s'" % state)
        if self.name != job.name:
            updates.append("name='%s'" % self.name)
        if self.user != job.user:
            updates.append("user='%s'" % self.user)
        if self.queue != job.queue:
            updates.append("queue='%s'" % self.queue)
        if self.project is not None:
            if self.project != job.project:
                updates.append("project='%s'" % self.project)
        if self.processors is not None:
            if self.processors != job.processors:
                updates.append("processors=%d" % self.processors)
        if self.requested_wall_time != job.requested_wall_time:
            updates.append("requestedWallTime=%d" % self.requested_wall_time)

        submit_time = self._updatedTime(self.submit_time,job.submit_time)
        if submit_time is not None:
            updates.append("submitTime='%s'" % submit_time.strftime("%Y-%m-%d %H:%M:%S"))
        start_time = self._updatedTime(self.start_time,job.start_time)
        if start_time is not None:
            updates.append("startTime='%s'" % start_time.strftime("%Y-%m-%d %H:%M:%S"))
        end_time = self._updatedTime(self.end_time,job.end_time)
        if end_time is not None:
            updates.append("endTime='%s'" % end_time.strftime("%Y-%m-%d %H:%M:%S"))

        if include_time and self.time != job.time:
            logger.debug("  time %s -> %s",job.time,self.time)
            updates.append("time='%s'" % self.time.strftime("%Y-%m-%d %H:%M:%S"))

        if len(updates) == 0:
            return None
        else:
            return ",".join(updates)

    def _updatedTime(self, new_time, old_time):
        if new_time == old_time:
            return None
        if new_time is None:
            return None
        if old_time is None:
            return new_time
        if new_time < old_time:  # assumes that the earlier time is better
            return new_time
        return None

    def fromGlue2Json(self, system, doc):
        self.system = system
        self.id = doc["LocalIDFromManager"]
        state = doc["State"][0]
        if state == "ipf:pending":
            self.state = Job.PENDING
        elif state == "ipf:held":
            self.state = Job.PENDING
        elif state == "ipf:starting":
            self.state = Job.RUNNING
        elif state == "ipf:running":
            self.state = Job.RUNNING
        elif state == "ipf:suspended":
            self.state = Job.PENDING
        elif state == "ipf:terminating":
            self.state = Job.DONE
        elif state == "ipf:terminated":
            self.state = Job.DONE
        elif state == "ipf:finishing":
            self.state = Job.DONE
        elif state == "ipf:finished":
            self.state = Job.DONE
        elif state == "ipf:failed":
            self.state = Job.DONE
        elif state == "ipf:unknown":
            raise KarnakError("'unknown' state for job %s on %s" % (system,self.id))
        else:
            raise KarnakError("unknown state '%s' for job %s on %s" % (state,system,self.id))
        try:
            self.name = doc["Name"]
        except KeyError:
            logger.debug("didn't find name for job %s on %s",self.id,system)
        try:
            self.user = doc["LocalOwner"]
        except KeyError:
            logger.debug("didn't find user for job %s on %s",self.id,system)
        try:
            self.queue = doc["Queue"]
        except KeyError:
            logger.debug("didn't find queue for job %s on %s",self.id,system)
        try:
            self.project = doc["UserDomain"]
        except KeyError:
            try:
                self.project = doc["Extension"]["LocalAccount"]
            except KeyError:
                logger.debug("didn't find project for job %s on %s",self.id,system)
        try:
            self.processors = doc["RequestedSlots"]
        except KeyError:
            pass
        # RequestedTotalWallTime is seconds * slots
        try:
            total_wall_time = doc["RequestedTotalWallTime"]
            self.requested_wall_time = total_wall_time / self.processors
        except KeyError:
            logger.debug("didn't find requested total wall time for job %s on %s",self.id,system)

        self.time = strToDateTime(doc["CreationTime"])
        try:
            self.submit_time = strToDateTime(doc["SubmissionTime"])
        except KeyError:
            try:
                self.submit_time = strToDateTime(doc["ComputingManagerSubmissionTime"])
            except KeyError:
                pass
        try:
            self.start_time = strToDateTime(doc["StartTime"])
        except KeyError:
            pass
        try:
            self.end_time = strToDateTime(doc["EndTime"])
        except KeyError:
            try:
                self.end_time = strToDateTime(doc["ComputingManagerEndTime"])
            except KeyError:
                pass

##############################################################################################################

class QueueState(object):
    def __init__(self, system=None, time=None, jobs=[]):
        self.system = system
        self.time = time  # datetime
        self.job_ids = map(lambda job: job.id,jobs)
        self.jobs = {}
        for job in jobs:
            self.jobs[job.id] = job

    def __str__(self):
        state_str = "system %s at %s with %d jobs\n" % \
                    (self.system,datetime.datetime.utcfromtimestamp(self.time).isoformat(),len(self.job_ids))
        for job_id in self.job_ids:
            if job_id in self.jobs:
                state_str += str(self.jobs[job_id])
            else:
                state_str += "job %s on %s\n" % (job_id,self.system)
        return state_str

    def getSqlDef(cls):
        return "system varchar(80) NOT NULL, time datetime NOT NULL, jobIds text"
    getSqlDef = classmethod(getSqlDef)

    def toSql(self):
        return "'" + self.system + "', " + \
            "'" + self.time.strftime("%Y-%m-%d %H:%M:%S") + "', " + \
            "'" + " ".join(self.job_ids) + "'"

    def fromSql(self, row):
        self.system = row[0]
        self.time = row[1]
        self.job_ids = row[2].split()
        self.jobs = {}

    def fromOrigSql(self, row):
        self.system = row[0]
        self.time = epochToNaiveDateTime(row[1])
        self.job_ids = row[2].split()
        self.jobs = {}
                
#######################################################################################################################

class System(object):
    def __init__(self):
        self.name = None
        self.time = None
        self.queues = {}
        self.processors = -1
        self.procs_per_node = -1
        self.allocation_size = -1

    def __str__(self):
        sstr = "system "+self.name+"\n"
        sstr = sstr + "  at time "+datetime.datetime.utcfromtimestamp(self.time).isoformat()+"\n"
        sstr = sstr + "  processors:     "+str(self.processors)+"\n"
        sstr = sstr + "  procs per node: "+str(self.procs_per_node)+"\n"
        for queue_name in sorted(self.queues.keys()):
            sstr = sstr + str(self.queues[queue_name])
        return sstr

    def getSqlDef(cls):
        return "system varchar(80) NOT NULL, time datetime, processors integer, procsPerNode integer"
    getSqlDef = classmethod(getSqlDef)

    def toSql(self):
        try:
            mytime = self.time.strftime("%Y-%m-%d %H:%M:%S")
	except:
            mytime = ''
        return "'%s','%s',%d,%d" % (self.name,
                                    mytime,
                                    self.processors,
                                    self.procs_per_node)

    def fromSql(self, row):
        self.name = row[0]
        self.time = row[1]
        self.processors = row[2]
        self.procs_per_node = row[3]

#######################################################################################################################

class Queue(object):
    def __init__(self, system):
        self.system = system.name
        self.name = None
        self.default = False
        self.valid = True
        self.max_processors = -1
        self.max_wall_time = -1

    def __str__(self):
        qstr = "  queue "+self.name
        if not self.valid:
            qstr = qstr + " (invalid)"
        if self.default:
            qstr = qstr + " (default)"
        qstr = qstr + "\n"
        qstr = qstr + "    max processors:       "+str(self.max_processors)+"\n"
        qstr = qstr + "    max wall time (secs): "+str(self.max_wall_time)+"\n"
        return qstr

    def getSqlDef(cls):
        return "system varchar(80) NOT NULL, queue varchar(80) NOT NULL, maxProcessors int, maxWallTime int, isDefault bit(1), isValid bit(1)"
    getSqlDef = classmethod(getSqlDef)

    def toSql(self):
        return "'%s','%s',%d,%d,%d,%d" % \
               (self.system,self.name,self.max_processors,self.max_wall_time,self.default,self.valid)

    def fromSql(self, row):
        self.system = row[0]
        self.name = row[1]
        self.max_processors = row[2]
        self.max_wall_time = row[3]
        if row[4] == 1:
            self.default = True
        else:
            self.default = False
        if row[5] == 1:
            self.valid = True
        else:
            self.valid = False

#######################################################################################################################
