
import traceback

import MySQLdb
import MySQLdb.cursors

from karnak.database import Glue2Database
from karnak.job import Job

def fix(system):
    db = Glue2Database()
    db.connect()

    old_jobs = []
    cursor = db.conn.cursor()
    cursor.execute("select * from jobs where system='%s.teragrid.org'" % system)
    for row in cursor:
        job = Job()
        job.fromSql(row)
        old_jobs.append(job)
    print("found %d old jobs for %s" % (len(old_jobs),system))

    for old_job in old_jobs:
        new_job = db.readJob(system+".xsede.org",old_job.id)
        #print("old %s" % old_job)
        #print("new %s" % new_job)
        if new_job is not None:
            old_job.system = system+".xsede.org"
            #print(new_job.toSqlUpdate(old_job))
            try:
                db.updateJob(new_job,old_job)
            except:
                print(traceback.format_exc())
            old_job.system = system+".teragrid.org"

    cursor.close()
    db.close()

if __name__ == "__main__":
    #fix("longhorn.tacc")
    fix("lonestar4.tacc")
    #fix("blacklight.psc")
