#!/bin/env python

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
    def __init__(self):
        self.conn = None

        self.job_cache = {}
    
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
        cursor.execute("create table if not exists jobs ("+Job.getSqlDef()+", primary key (uid)" + 
                       ", unique key (system,id,submit_time))")

        # experiment - system/id may not be unique
        cursor.execute("create table if not exists job_states ("+JobState.getSqlDef()+
                       ", foreign key (job_uid) references jobs(uid)"+
                       ", primary key (job_uid,time,state))")
        #cursor.execute("CREATE TABLE IF NOT EXISTS job_positions (system varchar(80), jobId varchar(80), time datetime, position integer), primary key (system,job_id,time)")
        #cursor.execute("CREATE TABLE IF NOT EXISTS job_positions (job_id integer, time datetime, position integer), foreign key fk_jid(job_id) references jobs(uid) on update cascade on delete restrict, primary key (job_id,time)")

        # archive.py saves the last set of jobs received here
        #cursor.execute("create table if not exists last_jobs ("+Job.getSqlDef()+", primary key (system,id))")

        # used by archive.py
        #cursor.execute("create table if not exists last_queue_states ("+QueueState.getSqlDef()+", "+ \
        #               "primary key (system))")

        # generate.py uses to determine what experiences may need to be generated
        #cursor.execute("create table if not exists job_queue ("+Job.getSqlDef(True)+", primary key (system,id,time))")

        #cursor.execute("create table if not exists job_queue (job_uid integer, time datetime, primary key (job_uid,time))")
        cursor.execute("create table if not exists started_job_queue (job_uid integer, primary key (job_uid))")
        cursor.execute("create table if not exists done_job_queue (job_uid integer, primary key (job_uid))")

        # used by generate.py
        #cursor.execute("create table if not exists queue_states ("+QueueState.getSqlDef()+
        #               ", primary key (uid)"
        #               ", unique key (system,time))")

        # system information used by the web interface
        cursor.execute("create table if not exists system_info ("+System.getSqlDef()+", primary key (system))")

        # queue information used by the web interface
        cursor.execute("create table if not exists queue_info ("+Queue.getSqlDef()+", primary key (system,queue))")

        cursor.close()

    ###################################################################################################################

    # allow caller to provide last_job, if they have it
    def updateJob(self, job, last_job=None, table="jobs"):
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
            logger.debug("update "+table+" set "+update+" where system=%s and id=%s" % (job.system,job.id))
            cursor = self.conn.cursor()
            cursor.execute("update "+table+" set "+update+" where system=%s and id=%s",(job.system,job.id))
            cursor.close()
            return True
        else:
            #logger.debug("nothing to update for job %s on %s",job.id,job.system)
            return False

    # shortcut when we know the job isn't already in the database
    def writeJob(self, job, table="jobs"):
        #logger.debug("insert into "+table+" values ("+job.toSql()+")")
        cursor = self.conn.cursor()
        cursor.execute("insert into "+table+" values ("+job.toSql()+")")
        cursor.close()

    ###################################################################################################################

    def getJobs(self, system, ids):
        jobs = []
        for id in ids:
            job = self.getJob(system,id)
            if job is not None:
                jobs.append(job)
        return jobs

    def getJob(self, system, id):
        if (system,id) in self.job_cache:
            return self.job_cache[(system,id)]
        job = self.readJob(system,id)
        if job is None:
            return job
        if job.end_time > 0:
            self.job_cache[(system,id)] = job
        if len(self.job_cache) >= 10000:
            self.job_cache = {}
        return job
        
    def readJobs(self, system, ids):
        jobs = []
        for id in ids:
            job = self.readJob(system,id)
            if job is not None:
                jobs.append(job)
        return jobs

    def readJob(self, system, id):
        cursor = self.conn.cursor()
        cursor.execute("select * from jobs where system=%s and id=%s",(system,id))
        row = cursor.fetchone()
        if row == None:
            logger.warn("couldn't read job %s on %s" % (id,system))
            cursor.close()
            return None
        job = Job()
        job.fromSql(row)
        cursor.close()

        return job

    ###################################################################################################################

    def writeLastQueueState(self, state):
        cursor = self.conn.cursor()
        # transaction?
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
        # transaction?
        cursor.execute("delete from last_jobs where system=%s",(system))
        for job in jobs:
            cursor.execute("insert into last_jobs values ("+job.toSql()+")")
        cursor.close()

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

    def writeJobQueue(self, job):
        cursor = self.conn.cursor()
        cursor.execute("insert into job_queue values ("+job.toSql(True)+")")
        cursor.close()

    ###################################################################################################################

    def writeQueueState(self, state):
        cursor = self.conn.cursor()
        cursor.execute("insert into queue_states values ("+state.toSql()+")")
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

        # archive.py saves all jobs here (how slow will this be to update?)
        cursor.execute("create table if not exists jobs ("+WaitTimeExperience.getSqlDef()+", primary key (system,id,submitTime))")

        # archive.py saves the last set of jobs received here
        cursor.execute("create table if not exists last_jobs ("+Job.getSqlDef()+", primary key (system,id))")

        # used by archive.py
        cursor.execute("create table if not exists last_queue_states ("+QueueState.getSqlDef()+", "+ \
                       "primary key (system))")

        # generate.py uses to determine what experiences may need to be generated
        cursor.execute("create table if not exists job_queue ("+Job.getSqlDef(True)+", primary key (system,id,time))")

        # used by generate.py (jobIds contains a space-separated list of job identifiers in the order they are queued)
        cursor.execute("create table if not exists queue_states (system varchar(80), time real, jobIds text, "+ \
                       "primary key (system,time))")

        # system information used by the web interface
        cursor.execute("create table if not exists system_info ("+System.getSqlDef()+", primary key (system))")

        # queue information used by the web interface
        cursor.execute("create table if not exists queue_info ("+Queue.getSqlDef()+", primary key (system,queue))")

        cursor.close()

    ###################################################################################################################











def readSystemNames(conn=None):
    if conn != None:
        return _readSystemNames(conn)
    connection = connect()
    names = _readSystemNames(connection)
    connection.close()
    return names

def _readSystemNames(conn):
    cur_time = time.time()
    cursor = conn.cursor()
    cursor.execute("select system from system_info where time > %s order by system asc",(cur_time-24*60*60))
    systems = []
    for row in cursor:
        systems.append(row[0])
    cursor.close()
    return systems

def readQueueNames(system, conn=None):
    if conn != None:
        return _readQueueNames(system,conn)
    connection = connect()
    names = _readQueueNames(system,connection)
    connection.close
    return names

def _readQueueNames(system, conn):
    cur_time = time.time()
    cursor = conn.cursor()
    cursor.execute("select queue from queue_info where system=%s",(system))
    queues = []
    for row in cursor:
        queues.append(row[0])
    cursor.close()
    return queues

#######################################################################################################################

def readCurrentQueueState(system, conn=None):
    if conn != None:
        return _readCurrentQueueState(system,conn)
    connection = connect()
    state = _readCurrentQueueState(system,connection)
    connection.close()
    return state

def _readCurrentQueueState(system, conn):
    queue = _readLastQueueState(system,conn)
    if queue == None:
        return None
    cur_time = time.time()
    if cur_time - queue.time > CURRENT_AGE_SECS:
        logger.warn("queue state for %s is too old" % system)
        logger.debug("  %d > %d" % (cur_time-queue.time,CURRENT_AGE_SECS))
        return None
    return queue
    
def readCurrentQueueStates(conn=None):
    if conn != None:
        return _readCurrentQueueStates(conn)
    connection = connect()
    state = _readCurrentQueueStates(connection)
    connection.close()
    return state

def _readCurrentQueueStates(conn):
    last_states = _readLastQueueStates(conn)
    current_states = {}
    cur_time = time.time()
    for state in last_states.values():
        if cur_time - state.time > CURRENT_AGE_SECS:
            logger.warn("queue state for "+state.system+" is too old")
            logger.debug("  "+str(cur_time-state.time)+" > "+str(CURRENT_AGE_SECS))
            continue
        current_states[state.system] = state
    return current_states

def readLastQueueState(system, conn=None):
    if conn != None:
        return _readLastQueueState(system,conn)
    connection = connect()
    state = _readLastQueueState(system,connection)
    connection.close()
    return state

def _readLastQueueState(system, conn):
    glue_jobs = _readLastGlue2Jobs(system,conn)
    state = None
    for glue_job in glue_jobs:
        if state is None:
            state = QueueState()
            state.system = glue_job.system
            state.time = glue_job.time
        job = Job()
        job.fromGlue2Job(glue_job)
        state.jobIds.append(job.id)
        state.jobs.append(job)
    cursor.close()

    return state
                                       
def readLastQueueStates(conn=None):
    if conn != None:
        return _readLastQueueStates(conn)
    connection = connect()
    state = _readLastQueueStates(connection)
    connection.close()
    return state

def _readLastQueueStates(conn):
    states = {}

    cursor = conn.cursor()
    cursor.execute("select * from last_glue2_jobs")
    for row in cursor:
        glue_job = Glue2Job()
        glue_job.fromSql(row)
        job = Job()
        job.fromGlue2Job(glue_job)
        if job.system not in states:
            state = QueueState()
            state.system = glue_job.system
            state.time = glue_job.time
        states[state.system] = state
        states[job.system].jobIds.append(job.id)
        states[job.system].jobs.append(job)
    cursor.close()

    return states

#######################################################################################################################

def readQueueStateBefore(system, time, conn=None):
    if conn != None:
        return _readQueueStateBefore(system,time,conn)
    connection = connect()
    state = _readQueueStateBefore(system,time,connection)
    connection.close()
    return state

def _readQueueStateBefore(system, time, conn):
    state = None
    cursor = conn.cursor()
    cursor.execute("select * from queue_states where system=%s and time<%s order by time desc limit 1",
                   (system,time))
    row = cursor.fetchone()
    if row == None:
        return None
    state = QueueState()
    state.fromSql(row)
    cursor.close()
    return state

def readQueueStateAfter(system, time, conn=None):
    if conn != None:
        return _readQueueStateAfter(system,time,conn)
    connection = connect()
    state = _readQueueStateAfter(system,time,connection)
    connection.close()
    return state

def _readQueueStateAfter(system, time, conn):
    state = None
    cursor = conn.cursor()
    cursor.execute("select * from queue_states where system=%s and time>%s order by time asc limit 1",
                   (system,time))
    row = cursor.fetchone()
    if row == None:
        return None
    state = QueueState()
    state.fromSql(row)
    cursor.close()
    return state

#######################################################################################################################

def readSubmittedJobs(system, start, end, conn):
    states = [JobState.PENDING]
    return readJobsByState(system,start,end,states,conn)

def readStartedJobs(system, start, end, conn):
    states = [JobState.RUNNING]
    return readJobsByState(system,start,end,states,conn)

def readEndedJobs(system, start, end, conn):
    states = [JobState.TERMINATED, JobState.FINISHED, JobState.DISAPPEARED]
    return readJobsByState(system,start,end,states,conn)

def readJobsByState(system, start, end, states, conn):
    jobIds = set()
    try:
        cursor = conn.cursor()

        command = "select distinct(id) from job_states where system='"+system+"'";
        if len(states) > 0:
            command = command + " and (";
            for i in range(0,len(states)):
                if i > 0:
                    command = command + " or "
                command = command + "state='"+states[i]+"'"
            command = command + ")"
        command = command + " and time >= "+str(start)+" and time < "+str(end)
        cursor.execute(command)

        for row in cursor:
            jobIds.add(row[0])

        cursor.close()
    except Exception, e:
        logger.error(traceback.format_exc())
        return None

    return readJobs(system,jobIds,conn)

job_dict = {}
max_job_dict_size = 100000

def _jobIsComplete(job):
    if job == None:
        return False
    if job.getSubmitTime() == -1:
        return False
    # not all jobs start
    #if job.getStartTime() == -1:
    #    return False
    if job.getEndTime() == -1:
        return False
    return True

def getJob(system, id, conn=None):
    global job_dict

    if (system,id) in job_dict:
        return job_dict[(system,id)]
    job = readJob(system,id,conn)
    if _jobIsComplete(job):
        if len(job_dict) > max_job_dict_size:
            job_dict = {}
        job_dict[(system,id)] = job
    return job

def readJob(system, id, conn=None):
    if conn != None:
        return _readJob(system,id,conn)
    connection = connect()
    state = _readJob(system,id,connection)
    connection.close()
    return state

def _readJob(system, id, conn):
    cursor = conn.cursor()

    cursor.execute("select * from job_descriptions where system=%s and id=%s",(system,id))
    row = cursor.fetchone()
    if row == None:
        logger.warn("couldn't read job %s on %s" % (id,system))
        return None
    job = Job()
    job.fromSqlDescription(row)

    cursor.execute("select * from job_states where system=%s and id=%s order by time asc",(system,id))
    for row in cursor:
        state = JobState()
        state.fromSql(row)
        job.states.append(state)

    cursor.close()

    return job


def readJobs(system, ids, conn=None):
    if conn != None:
        return _readJobs(system,ids,conn)
    connection = connect()
    state = _readJobs(system,ids,connection)
    connection.close()
    return state

def _readJobs(system, ids, conn):
    cursor = conn.cursor()

    jobs = []
    for id in ids:
        cursor.execute("select * from job_descriptions where system=%s and id=%s",(system,id))
        row = cursor.fetchone()
        if row == None:
            logger.warn("couldn't read job %s on %s" % (id,system))
            continue
        job = Job()
        job.fromSqlDescription(row)

        cursor.execute("select * from job_states where system=%s and id=%s order by time asc",(system,id))
        for row in cursor:
            state = JobState()
            state.fromSql(row)
            job.states.append(state)

        jobs.append(job)

    cursor.close()

    return jobs


def getJobs(system, ids, conn=None):
    if conn != None:
        return _getJobs(system,ids,conn)
    connection = connect()
    state = _getJobs(system,ids,connection)
    connection.close()
    return state
        
def _getJobs(system, ids, conn):
    global job_dict
    
    cursor = conn.cursor()

    jobs = []
    num_read = 0
    num_saved = 0
    for id in ids:
        if (system,id) in job_dict:
            jobs.append(job_dict[(system,id)])
            continue

        cursor.execute("select * from job_descriptions where system=%s and id=%s",(system,id))
        row = cursor.fetchone()
        if row == None:
            logger.warn("couldn't read job %s on %s" % (id,system))
            continue
        job = Job()
        job.fromSqlDescription(row)

        cursor.execute("select * from job_states where system=%s and id=%s order by time asc",(system,id))
        for row in cursor:
            state = JobState()
            state.fromSql(row)
            job.states.append(state)

        num_read += 1
        if _jobIsComplete(job):
            num_saved += 1
            if len(job_dict) > max_job_dict_size:
                logger.info("clearing job cache")
                job_dict = {}
            job_dict[(system,id)] = job

        jobs.append(job)

    cursor.close()

    logger.info("read %d of %d jobs (saved %d of them)" % (num_read,len(ids),num_saved))

    return jobs

#######################################################################################################################

def writeRunTimeExperience(exp, conn=None):
    if conn != None:
        return _writeRunTimeExperience(exp,"runtime_experiences",conn)
    connection = connect()
    state = _writeRunTimeExperience(exp,"runtime_experiences",connection)
    connection.close()
    return state

def writeRunTimeExperienceToQueue(exp, conn=None):
    if conn != None:
        return _writeRunTimeExperience(exp,"runtime_experience_queue",conn)
    connection = connect()
    state = _writeRunTimeExperience(exp,"runtime_experience_queue",connection)
    connection.close()
    return state

def _writeRunTimeExperience(exp, table, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("insert into "+table+" values ("+exp.toSql()+")")
    except Exception, e:
        logger.error(traceback.format_exc())
    cursor.close()

#######################################################################################################################

def writeSubmittedExperience(exp, conn=None):
    if conn != None:
        return _writeWaitTimeExperience(exp,"submitted_experiences",conn)
    connection = connect()
    state = _writeWaitTimeExperience(exp,"submitted_experiences",connection)
    connection.close()
    return state

def writeSubmittedExperienceToQueue(exp, conn=None):
    if conn != None:
        return _writeWaitTimeExperience(exp,"submitted_experience_queue",conn)
    connection = connect()
    state = _writeWaitTimeExperience(exp,"submitted_experience_queue",connection)
    connection.close()
    return state

def writeUnsubmittedExperience(exp, conn=None):
    if conn != None:
        return _writeWaitTimeExperience(exp,"unsubmitted_experiences",conn)
    connection = connect()
    state = _writeWaitTimeExperience(exp,"unsubmitted_experiences",connection)
    connection.close()
    return state

def writeUnsubmittedExperienceToQueue(exp, conn=None):
    if conn != None:
        return _writeWaitTimeExperience(exp,"unsubmitted_experience_queue",conn)
    connection = connect()
    state = _writeWaitTimeExperience(exp,"unsubmitted_experience_queue",connection)
    connection.close()
    return state

def _writeWaitTimeExperience(exp, table, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("insert into "+table+" values ("+exp.toSql()+")")
    except Exception, e:
        logger.error(traceback.format_exc())
    cursor.close()


def readUnsubmittedExperiences(system, start, end, conn):
    experiences = []
    try:
        cursor = conn.cursor()

        command = "select * from unsubmitted_experiences where system='"+system+"'" + \
                  " and time >= "+str(start)+" and time < "+str(end)
        cursor.execute(command)

        for row in cursor:
            exp = WaitTimeExperience()
            exp.fromSql(row)
            experiences.append(exp)

        cursor.close()
    except Exception, e:
        logger.error(traceback.format_exc())
        return None

    return experiences

def readUnsubmittedExperiencesForId(system, id, conn):
    experiences = []
    try:
        cursor = conn.cursor()

        command = "select * from unsubmitted_experiences where system='"+system+"' and id='"+id+"'"
        cursor.execute(command)

        for row in cursor:
            exp = WaitTimeExperience()
            exp.fromSql(row)
            experiences.append(exp)

        cursor.close()
    except Exception, e:
        logger.error(traceback.format_exc())
        return None

    return experiences

#######################################################################################################################
