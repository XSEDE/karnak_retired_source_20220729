
import datetime
import logging

from error import UnknownJobError
from job import hms
from job import Job

#######################################################################################################################

def createRunTimeSchema(system, queues):
    features = []
    #features.append(StringFeatureSchema("system",True))
    #features.append(StringFeatureSchema("id",True))
    features.append(StringFeatureSchema("name"))
    features.append(StringFeatureSchema("user"))
    features.append(CategoryFeatureSchema("queue",queues))
    features.append(StringFeatureSchema("project"))
    features.append(IntFeatureSchema("processors"))
    features.append(IntFeatureSchema("requestedWallTime"))
    features.append(LongFeatureSchema("startTime"))
    features.append(LongFeatureSchema("runTime"))

    return RunTimeExperienceSchema(features)

#######################################################################################################################

#class SqlExperienceSchema(ExperienceSchema):
#    def __init__(self, features):
#        ExperienceSchema.__init__(self,features)

    # is this even necessary?
#    def fromSql(self, row):
#        values = []
#        for value in row:
#            values.append(value)

#    def toSql(self, exp):
#        return "'" + "','".join(map(lambda value: str(value),list(exp))) + "'"
        
#######################################################################################################################

#class RunTimeExperienceSchema(SqlExperienceSchema):
#    def __init__(self, features):
#        SqlExperienceSchema.__init__(self,features)

#    def __str__(self):
#        expStr = "experience "+self.id+" on "+self.system+":\n"
#        if self.name != "":
#            expStr = expStr + "  name: "+self.name+"\n"
#        expStr = expStr + "  user: "+self.user+"\n"
#        expStr = expStr + "  queue: "+self.queue+"\n"
#        expStr = expStr + "  project: "+self.project+"\n"
#        expStr = expStr + "  processors: "+str(self.processors)+"\n"
#        expStr = expStr + "  requested wall time: "+hms(self.requestedWallTime)+"\n"

#        expStr = expStr + "  end time: "+datetime.datetime.utcfromtimestamp(self.endTime).isoformat()+"\n"
#        expStr = expStr + "  run time: "+hms(self.runTime)+"\n"

#        return expStr


#    def createFromJob(self, job):
#        return createExperience((job.system,job.id,job.name,job.user,job.queue,job.project,
#                                 job.processors,job.requestedWallTime,job.getStartTime(),job.getEndTime()))

#    def getTime(self):
#        return self.endTime

#######################################################################################################################

def createUnsubmittedWaitTimeSchema(system, queues):
    features = []
    features.append(StringFeatureSchema("system",True))
    #features.append(StringFeatureSchema("id",True))
    features.append(CategoryFeatureSchema("queue",queues))
    features.append(IntFeatureSchema("processors"))
    features.append(IntFeatureSchema("requestedWallTime"))
    features.append(LongFeatureSchema("submitTime"))
    features.append(LongFeatureSchema("count"))
    features.append(FloatFeatureSchema("work"))
    features.append(BooleanFeatureSchema("jobsRunning"))
    features.append(LongFeatureSchema("waitTime"))

    return UnsubmittedWaitTimeExperienceSchema(features)

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

    def fromJob(self, job):
        self.id = job.id
        self.name = job.name
        self.user = job.user
        self.queue = job.queue
        self.project = job.project
        self.processors = job.processors
        self.requestedWallTime = job.requestedWallTime
        self.startTime = job.getStartTime()

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
                runningTime = queueState.time - job.getStartTime()
                if runningTime > job.requestedWallTime:
                    runningTime = job.requestedWallTime
            else:
                runningTime = 0
            self.work = self.work + job.processors * (job.requestedWallTime - runningTime)
            if job.user == self.user:
                self.userCount = self.userCount + 1
                self.userWork = self.userWork = job.processors * (job.requestedWallTime - runningTime)
        raise UnknownJobError(queueState.system,jobId)

######################################################################
