
import sys

from carnac.database import Glue2Database
from carnac.job import *

#######################################################################################################################

def getQueueState():
    db = Glue2Database()
    db.connect()
    state = db.readLastQueueState(sys.argv[1])
    state.jobs = db.readLastJobs(state.system)
    db.close()
    return state

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: qstat <system name>")
        sys.exit(1)

    state = getQueueState()

    print("system %s at %s with %d jobs\n" % \
          (state.system,datetime.datetime.utcfromtimestamp(state.time).isoformat(),len(state.job_ids)))
    #print(str(state))
            
