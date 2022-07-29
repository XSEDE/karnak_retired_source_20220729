

#######################################################################################################################

class CarnacError(Exception):
    def __init__(self, message="Carnac Error"):
        Exception.__init__(self,message)

#######################################################################################################################

class UnknownJobError(Exception):
    def __init__(self, system, job_id):
        Exception.__init__(self,"job %s is unknown on system " % (job_id,system))
        self.system = system
        self.jobId = job_id

#######################################################################################################################
