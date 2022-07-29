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

from error import UnknownJobError
from job import hms
from job import Job

logger = logging.getLogger(__name__)

#######################################################################################################################

class WaitTimeExperience(object):
    def __init__(self):
        self.system = None
        self.id = None
        self.name = None
        self.user = None
        self.queue = None
        self.project = None
        self.processors = None
        self.requested_wall_time = None

        self.time = None

        # don't include running jobs (did previously)
        self.count_ahead = None
        self.work_ahead = None

        self.count_running = None
        self.processors_running = None
        self.work_running = None

        # don't include running jobs (did previously)
        self.count_ahead_user = None
        self.work_ahead_user = None

        self.count_running_user = None
        self.processors_running_user = None
        self.work_running_user = None

        self.count_ahead_project = None
        self.work_ahead_project = None

        self.count_running_project = None
        self.processors_running_project = None
        self.work_running_project = None

        # add held state to Job and use it?

        # to capture fair share, could include recent history of jobs run by user/project

        # if this queue is high priority
        self.count_ahead_queue = None
        self.work_ahead_queue = None

        # if this queue is low priority
        self.count_other_queues = None
        self.work_other_queues = None

        # try to capture backfilling
        self.count_ahead_less_equal_procs = None
        self.work_ahead_less_equal_procs = None
        self.count_ahead_less_equal_work = None
        self.work_ahead_less_equal_work = None

        self.jobs_running = None

        self.start_time = None

    def __str__(self):
        expStr = "experience for "+self.id+" on "+self.system+":\n"
        if self.name != "":
            expStr = expStr + "  name:                                  %s\n" % self.name
        expStr = expStr + "  user:                                  %s\n" % self.user
        expStr = expStr + "  queue:                                 %s\n" % self.queue
        expStr = expStr + "  project:                               %s\n" % self.project
        expStr = expStr + "  processors:                            "+str(self.processors)+"\n"
        expStr = expStr + "  requested wall time:                   "+hms(self.requested_wall_time)+"\n"
        expStr = expStr + "  time:                                  "+self.time.isoformat()+"\n"

        expStr = expStr + "  count ahead:                           "+str(self.count_ahead)+"\n"
        expStr = expStr + "  work ahead:                            "+str(self.work_ahead)+"\n"

        expStr = expStr + "  count running:                         "+str(self.count_running)+"\n"
        expStr = expStr + "  processors running:                    "+str(self.processors_running)+"\n"
        expStr = expStr + "  work running:                          "+str(self.work_running)+"\n"

        expStr = expStr + "  count ahead for user:                  "+str(self.count_ahead_user)+"\n"
        expStr = expStr + "  work ahead for user:                   "+str(self.work_ahead_user)+"\n"

        expStr = expStr + "  count running for user:                "+str(self.count_running_user)+"\n"
        expStr = expStr + "  processors running for user:           "+str(self.processors_running_user)+"\n"
        expStr = expStr + "  work running user:                     "+str(self.work_running_user)+"\n"

        expStr = expStr + "  count ahead for project:               "+str(self.count_ahead_project)+"\n"
        expStr = expStr + "  work ahead for project:                "+str(self.work_ahead_project)+"\n"

        expStr = expStr + "  count running for project:             "+str(self.count_running_project)+"\n"
        expStr = expStr + "  processors running for project:        "+str(self.processors_running_project)+"\n"
        expStr = expStr + "  work running project:                  "+str(self.work_running_project)+"\n"

        expStr = expStr + "  count ahead in queue:                  "+str(self.count_ahead_queue)+"\n"
        expStr = expStr + "  work ahead in queue:                   "+str(self.work_ahead_queue)+"\n"
        expStr = expStr + "  count ahead in other queues:           "+str(self.count_other_queues)+"\n"
        expStr = expStr + "  work ahead in other queues:            "+str(self.work_other_queues)+"\n"

        expStr = expStr + "  count lte processors ahead in queue:   " + \
                 str(self.count_ahead_less_equal_procs)+"\n"
        expStr = expStr + "  work lte processors ahead in queue:    " + \
                 str(self.work_ahead_less_equal_procs)+"\n"
        expStr = expStr + "  count lte work ahead in queue:         " + \
                 str(self.count_ahead_less_equal_work)+"\n"
        expStr = expStr + "  work lte work ahead in queue:          " + \
                 str(self.work_ahead_less_equal_work)+"\n"

        expStr = expStr + "  jobs are running:                      "+str(self.jobs_running)+"\n"

        expStr = expStr + "  start time:                            "
        if self.start_time is None:
            expStr = expStr + "(none)\n"
        else:
            expStr = expStr + self.start_time.isoformat()+"\n"

        return expStr

    def getSqlDef(cls):
        return "system varchar(80), id varchar(80), name text, user text, queue text, project text, " + \
            "processors integer, requestedWallTime integer, time datetime, " + \
            "countAhead integer, workAhead real, " + \
            "countRunning integer, processorsRunning integer, workRunning real, " + \
            "countAheadUser integer, workAheadUser real, " + \
            "countRunningUser integer, processorsRunningUser integer, workRunningUser real, " + \
            "countAheadProject integer, workAheadProject real, " + \
            "countRunningProject integer, processorsRunningProject integer, workRunningProject real, " + \
            "countAheadQueue integer, workAheadQueue real, " + \
            "countOtherQueues integer, workOtherQueues real, " + \
            "countAheadLessEqualProcs integer, workAheadLessEqualProcs real, " + \
            "countAheadLessEqualWork integer, workAheadLessEqualWork real, " + \
            "jobsRunning boolean, " + \
	    "startTime datetime"
    getSqlDef = classmethod(getSqlDef)

    def fromJobAndQueueState(self, job, queue_state):
        self._setJob(job)
        self._setQueueState(queue_state)

    def _setJob(self, job):
        self.id = job.id
        self.name = job.name
        self.user = job.user
        self.queue = job.queue
        self.project = job.project
        self.processors = job.processors
        self.requested_wall_time = job.requested_wall_time
        self.start_time = job.start_time

    def work(self):
        if self.requested_wall_time is None:
            logger.warn("requested wall time is None for experience %s on %s" % (self.id,self.system))
            return 0
        return self.processors * self.requested_wall_time

    def _setQueueState(self, queue_state):
	self.system = queue_state.system
        self.time = queue_state.time

        queue_state.jobs = {id:job for (id,job) in queue_state.jobs.items() if job.processors is not None}

        job_ids_ahead = []
        for id in queue_state.job_ids:
            if id == self.id:
                break
            job_ids_ahead.append(id)
        jobs_ahead = filter(lambda job: job.pendingAtTime(queue_state.time),
                            map(lambda id: queue_state.jobs[id],
                                filter(lambda job_id: job_id in queue_state.jobs,job_ids_ahead)))
        self.count_ahead = len(jobs_ahead)
        self.work_ahead = sum(map(lambda job: job.work(),jobs_ahead))

        running_jobs = filter(lambda job: job.runningAtTime(queue_state.time),queue_state.jobs.values())
        self.count_running = len(running_jobs)

        self.processors_running = sum(map(lambda job: job.processors,running_jobs))
        self.work_running = sum(map(lambda job: job.remainingWork(queue_state.time),running_jobs))

        user_jobs_ahead = filter(lambda job: job.user == self.user,jobs_ahead)
        self.count_ahead_user = len(user_jobs_ahead)
        self.work_ahead_user = sum(map(lambda job: job.work(),user_jobs_ahead))

        user_running_jobs = filter(lambda job: job.user == self.user,running_jobs)
        self.count_running_user = len(user_running_jobs)
        self.processors_running_user = sum(map(lambda job: job.processors,user_running_jobs))
        self.work_running_user = sum(map(lambda job: job.remainingWork(queue_state.time),user_running_jobs))

        if self.project is not None:
            project_jobs_ahead = filter(lambda job: job.project == self.project,jobs_ahead)
            self.count_ahead_project = len(project_jobs_ahead)
            self.work_ahead_project = sum(map(lambda job: job.work(),project_jobs_ahead))

            project_running_jobs = filter(lambda job: job.project == self.project,running_jobs)
            self.count_running_project = len(user_running_jobs)
            self.processors_running_project = sum(map(lambda job: job.processors,user_running_jobs))
            self.work_running_project = sum(map(lambda job: job.remainingWork(queue_state.time),user_running_jobs))

        queue_jobs_ahead = filter(lambda job: job.queue == self.queue,jobs_ahead)
        self.count_ahead_queue = len(queue_jobs_ahead)
        self.work_ahead_queue = sum(map(lambda job: job.work(),queue_jobs_ahead))

        pending_jobs = filter(lambda job: job.id != self.id and job.pendingAtTime(queue_state.time),
                              queue_state.jobs.values())
        other_queues_jobs = filter(lambda job: job.queue != self.queue,pending_jobs)
        self.count_other_queues = len(other_queues_jobs)
        self.work_other_queues = sum(map(lambda job: job.work(),other_queues_jobs))

        less_equal_procs_jobs_ahead = filter(lambda job: job.processors <= self.processors,jobs_ahead)
        self.count_ahead_less_equal_procs = len(less_equal_procs_jobs_ahead)
        self.work_ahead_less_equal_procs = sum(map(lambda job: job.work(),less_equal_procs_jobs_ahead))

        less_equal_work_jobs_ahead = filter(lambda job: job.work() <= self.work(),jobs_ahead)
        self.count_ahead_less_equal_work = len(less_equal_work_jobs_ahead)
        self.work_ahead_less_equal_work = sum(map(lambda job: job.work(),less_equal_work_jobs_ahead))

        # jobsRunning can only be false when there are jobs
        if len(queue_state.jobs) == 0:
            self.jobs_running = True
        else:
            self.jobs_running = False
            for job in queue_state.jobs.values():
                if job.runningAtTime(queue_state.time):
                    self.jobs_running = True
                    break

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

        self.count_ahead = row[9]
        self.work_ahead = row[10]
        self.count_running = row[11]
        self.processors_running = row[12]
        self.work_running = row[13]

        self.count_ahead_user = row[14]
        self.work_ahead_user = row[15]

        self.count_running_user = row[16]
        self.processors_running_user = row[17]
        self.work_running_user = row[18]

        self.count_ahead_project = row[19]
        self.work_ahead_project = row[20]

        self.count_running_project = row[21]
        self.processors_running_project = row[22]
        self.work_running_project = row[23]

        self.count_ahead_queue = row[24]
        self.work_ahead_queue = row[25]

        self.count_other_queues = row[26]
        self.work_other_queues = row[27]

        self.count_ahead_less_equal_procs = row[28]
        self.work_ahead_less_equal_procs = row[29]
        self.count_ahead_less_equal_work = row[30]
        self.work_ahead_less_equal_work = row[31]

        self.jobs_running = row[32]

        self.startTime = row[33]


    def toSql(self):
        sql_str = ""
        if self.system is None:
            sql_str += "NULL"
        else:
            sql_str += "'%s'" % self.system
        if self.id is None:
            sql_str += ", NULL"
        else:
            sql_str += ", '%s'" % self.id
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
        sql_str += ", '%s'" % self.time.strftime("%Y-%m-%d %H:%M:%S")

        sql_str += ", %d" % self.count_ahead
        sql_str += ", %d" % self.work_ahead
        sql_str += ", %d" % self.count_running
        sql_str += ", %d" % self.processors_running
        sql_str += ", %d" % self.work_running

        sql_str += ", %d" % self.count_ahead_user
        sql_str += ", %d" % self.work_ahead_user
        sql_str += ", %d" % self.count_running_user
        sql_str += ", %d" % self.processors_running_user
        sql_str += ", %d" % self.work_running_user

        if self.count_ahead_project is not None:
	    sql_str += ", " + str(self.count_ahead_project) + \
                       ", " + str(self.work_ahead_project) + \
                       ", " + str(self.count_running_project) + \
                      ", " + str(self.processors_running_project) + \
                      ", " + str(self.work_running_project)
        else:
	    sql_str += ", NULL, NULL, NULL, NULL, NULL"

        if self.count_ahead_queue is not None:
	    sql_str += ", " + str(self.count_ahead_queue) + \
                       ", " + str(self.work_ahead_queue) + \
                       ", " + str(self.count_other_queues) + \
                       ", " + str(self.work_other_queues)
        else:
	    sql_str += ", NULL, NULL, NULL, NULL"

        sql_str += ", " + str(self.count_ahead_less_equal_procs) + \
                   ", " + str(self.work_ahead_less_equal_procs) + \
                   ", " + str(self.count_ahead_less_equal_work) + \
                   ", " + str(self.work_ahead_less_equal_work)

        if self.jobs_running:
            sql_str = sql_str + ", 1"
        else:
            sql_str = sql_str + ", 0"

        sql_str += ", '%s'" % self.start_time.strftime("%Y-%m-%d %H:%M:%S")

	return sql_str

#######################################################################################################################
