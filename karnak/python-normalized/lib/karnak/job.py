
import calendar
import datetime
import json
import logging

from error import *
from util import *

logger = logging.getLogger(__name__)

##############################################################################################################

class Job(object):

    # simplified queue states
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    DISAPPEARED = "disappeared"

    def __init__(self):
        # "" and -1 instead of None for easy SQL writing
        self.system = ""
        self.id = ""
        self.state = ""
        self.name = ""
        self.user = ""
        self.queue = ""
        self.project = ""
        self.processors = -1
        self.requested_wall_time = -1  # seconds

        self.time = None               # times are datetime
        self.submit_time = None
        self.start_time = None
        self.end_time = None

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
        if self.name == "":
            job_str = job_str + "  name: %s\n" % self.name
        job_str = job_str + "  user: %s\n" % self.user
        job_str = job_str + "  queue: %s\n" % self.queue
        job_str = job_str + "  project: %s\n" % self.project
        job_str = job_str + "  processors: %d\n" % self.processors
        job_str = job_str + "  requested wall time: %s\n" % hms(self.requested_wall_time)

        if self.time != None:
            job_str = job_str + "  time: %s\n" % datetime.datetime.utcfromtimestamp(self.time).isoformat()

        if self.submit_time != None:
            job_str = job_str + "  submit time: %s\n" % self.submit_time.isoformat()
        if self.start_time != None:
            job_str = job_str + "  start time: %s\n" % self.start_time.isoformat()
        if self.end_time != None:
            job_str = job_str + "  end time: %s\n" % self.end_time.isoformat()

        return job_str

    def pendingAtTime(self, time):
        if time >= self.submit_time and time < self.start_time:
            return True
        return False

    def runningAtTime(self, time):
        if time >= self.start_time and time <= self.end_time:
            return True
        return False

    def getSqlDef(cls):
        sql_str = "system varchar(80) NOT NULL" + \
                  ", id varchar(40) NOT NULL" + \
                  ", name text" + \
                  ", user text" + \
                  ", queue text" + \
                  ", project text" + \
                  ", processors integer" + \
                  ", requested_wall_time integer" + \
                  ", submit_time datetime NOT NULL" + \
                  ", uid integer AUTO_INCREMENT"
        return sql_str
    getSqlDef = classmethod(getSqlDef)


    def toSql(self):
        sql_str = "'" + self.system + "', " + \
                  "'" + self.id + "', " + \
                  "'" + self.name + "', " + \
                  "'" + self.user + "', " + \
                  "'" + self.queue + "', " + \
                  "'" + self.project + "', " + \
                  str(self.processors) + ", " + \
                  str(self.requested_wall_time)
        if self.submit_time is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.submit_time.strftime("%Y-%m-%d %H:%M:%S")
        sql_str += ", %d" & self.uid
        return sql_str

    def fromSql(self, row):
        self.system = row[0]
        self.id = row[1]
        self.name = row[2]
        self.user = row[3]
        self.queue = row[4]
        self.project = row[5]
        self.processors = row[6]
        self.requested_wall_time = row[7]
        self.submit_time = row[8]
        self.uid = row[9]

    # shouldn't need to update the job, just the job states
    def oldToSqlUpdate(self, job, include_time=False):
        updates = []
        if self.system != job.system:
            raise CarnacError("can't update a job from a different system!")
        if self.id != job.id:
            raise CarnacError("can't update a job with a different id!")

        # job info in a queue state can be older than that received from job updates
        if self.state == Job.PENDING and job.state != Job.PENDING:
            logger.debug("ignoring pending job %s on %s - already have newer information",job.id,job.system)
            return None
        if self.state == Job.RUNNING and (job.state == Job.DONE or job.state == Job.DISAPPEARED):
            logger.debug("ignoring running job %s on %s - already have newer information",job.id,job.system)
            return None
        if self.state == Job.DISAPPEARED and job.state == Job.DONE:
            logger.debug("ignoring disappeared job %s on %s - already saw done state",job.id,job.system)
            return None

        if self.state != job.state:
            logger.debug("  state %s -> %s",job.state,self.state)
            updates.append("state='%s'" % self.state)
        if self.name != job.name:
            updates.append("name='%s'" % self.name)
        if self.user != job.user:
            updates.append("user='%s'" % self.user)
        if self.queue != job.queue:
            updates.append("queue='%s'" % self.queue)
        if self.project != job.project:
            updates.append("project='%s'" % self.project)
        if self.processors != job.processors:
            updates.append("processors=%d" % self.processors)
        if self.requested_wall_time != job.requested_wall_time:
            updates.append("requestedWallTime=%d" % self.requested_wall_time)
        if self.submit_time != -1 and self.submit_time != job.submit_time:
            logger.debug("  submitTime %s -> %s",job.submit_time,self.submit_time)
            updates.append("submitTime='%s'" % self.submit_time.strftime("%Y-%m-%d %H:%M:%S"))
        if self.start_time != -1 and self.start_time != job.start_time:
            logger.debug("  startTime %s -> %s",job.start_time,self.start_time)
            updates.append("startTime='%s'" % self.start_time.strftime("%Y-%m-%d %H:%M:%S"))
        if self.end_time != -1 and self.end_time != job.end_time:
            logger.debug("  endTime %s -> %s",job.end_time,self.end_time)
            updates.append("endTime='%s'" % self.end_time.strftime("%Y-%m-%d %H:%M:%S"))
        if include_time and self.time != job.time:
            logger.debug("  time %s -> %s",job.time,self.time)
            updates.append("time='%s'" % self.time.strftime("%Y-%m-%d %H:%M:%S"))

        if len(updates) == 0:
            return None
        else:
            return ",".join(updates)

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
            raise CarnacError("'unknown' state for job %s on %s" % (system,self.id))
        else:
            raise CarnacError("unknown state '%s' for job %s on %s" % (state,system,self.id))
        self.name = doc["Name"]
        self.user = doc["LocalOwner"]
        self.queue = doc["Queue"]
        try:
            self.project = doc["UserDomain"]
        except KeyError:
            try:
                self.project = doc["Extension"]["LocalAccount"]
            except KeyError:
                logger.debug("didn't find project for job %s on %s",self.id,system)
        self.processors = doc["RequestedSlots"]
        # RequestedTotalWallTime is seconds * slots
        total_wall_time = doc["RequestedTotalWallTime"]
        self.requested_wall_time = total_wall_time / self.processors

        self.time = toDateTime(doc["CreationTime"])
        try:
            self.submit_time = toDateTime(doc["SubmissionTime"])
        except KeyError:
            try:
                self.submit_time = toDateTime(doc["ComputingManagerSubmissionTime"])
            except KeyError:
                pass
        try:
            self.start_time = toDateTime(doc["StartTime"])
        except KeyError:
            pass
        try:
            self.end_time = toDateTime(doc["EndTime"])
        except KeyError:
            try:
                self.end_time = toDateTime(doc["ComputingManagerEndTime"])
            except KeyError:
                pass

##############################################################################################################

class JobState(object):
    def __init__(self):
        self.job_uid = None
        self.time = None
        self.state = None
        self.position = 0

    def getSqlDef(cls, include_time=False):
                  #", qs_uid integer" + \
        sql_str = "job_uid integer NOT NULL" + \
                  ", time datetime NOT NULL" + \
                  ", state enum('pending','running','done','disappeared')" + \
                  ", position integer" # 1 or higher when pending
        return sql_str
    getSqlDef = classmethod(getSqlDef)

    def toSql(self):
        return "%d,'%s','%s',%d" % (self.job_uid,self.time.strftime("%Y-%m-%d %H:%M:%S"),self.state,self.position)

    def fromSql(self, row):
        self.job_uid = row[0]
        self.time = row[1]
        self.state = row[2]
        self.position = row[3]

    def fromGlue2Json(self, job_uid, doc):
        self.job_uid = job_uid
        self.time = toDateTime(doc["CreationTime"])

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
            raise CarnacError("'unknown' state for job %s on %s" % (system,self.id))
        else:
            raise CarnacError("unknown state '%s' for job %s on %s" % (state,system,self.id))

        # for queue states, make sure that this is set
        self.position = doc.get("WaitingPosition",0)


##############################################################################################################

class QueueState(object):
    def __init__(self, system=None, time=-1, jobs=[]):
        self.system = system
        self.time = time  # seconds since the epoch
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

        # don't need?
    def getSqlDef(cls):
        return "system varchar(80) NOT NULL, time datetime NOT NULL, uid integer AUTO_INCREMENT"
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
        self.time = -1
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
        return "'%s','%s',%d,%d" % (self.name,
                                    self.time.strftime("%Y-%m-%d %H:%M:%S"),
                                    self.time,self.processors,self.procs_per_node)

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
