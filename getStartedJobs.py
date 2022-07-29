
import traceback

import MySQLdb
import MySQLdb.cursors

from karnak.database import Glue2Database, KarnakDatabase
from karnak.job import Job

def generate(system):
    db = Glue2Database()
    db.connect()
    db2 = Glue2Database()
    db2.connect()

    cursor = db.conn.cursor()
    cursor.execute("select * from jobs where system=%s",system)
    for row in cursor:
        job = Job()
        job.fromSql(row)
        if job.start_time is not None:
            db2.writeStartedJob(job)
    cursor.close()
    db.close()
    db2.close()

if __name__ == "__main__":
    #generate("blacklight.psc.xsede.org")
    generate("comet.sdsc.xsede.org")
    generate("gordon.sdsc.xsede.org")
    generate("lonestar4.tacc.xsede.org")
    generate("maverick.tacc.xsede.org")
    generate("stampede.tacc.xsede.org")
