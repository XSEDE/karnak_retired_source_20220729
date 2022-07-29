
import datetime

import MySQLdb
import MySQLdb.cursors

def _epochToStr(epoch):
    if epoch <= 0:
        return """\N"""
    dt = datetime.datetime.utcfromtimestamp(epoch)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def _connect():
    conn = MySQLdb.connect(user="karnak",db="glue2",
                           cursorclass=MySQLdb.cursors.SSCursor)
    conn.autocommit(True)
    return conn

# maybe 10 hours to complete?
def writeJobs():
    infile = open("/home/karnak/glue2/jobs_orig_sorted.txt","r")
    conn = _connect()
    for line in infile:
        #print(line)
        (system,id,state,name,user,queue,project,processors,req_wall_time,
         submit_epoch_str,start_epoch_str,end_epoch_str) = line.split("\t")
        submit_epoch = int(submit_epoch_str)
        start_epoch = int(start_epoch_str)
        end_epoch = int(end_epoch_str)

        if submit_epoch <= 0:
            continue
        if end_epoch > 0 and start_epoch <= 0:
            continue

        cursor = conn.cursor()
        sql = "insert into jobs values('%s','%s','%s','%s','%s','%s',%d,%d,'%s',NULL)" % \
              (system,id,name,user,queue,project,int(processors),int(req_wall_time),
               _epochToStr(submit_epoch))
        cursor.execute(sql)

        cursor.execute("select last_insert_id()")
        row = cursor.fetchone()
        if row is not None:
            job_uid = row[0]
        cursor.close()

        cursor = conn.cursor()
        cursor.execute("insert into job_states values (%d,'%s','%s',NULL)" %
                       (job_uid,_epochToStr(submit_epoch),"pending"))
        if start_epoch > 0:
            cursor.execute("insert into job_states values (%d,'%s','%s',NULL)" %
                           (job_uid,_epochToStr(start_epoch),"running"))
        if end_epoch > 0:
            if state == "done" or state == "disappeared":
                cursor.execute("insert into job_states values (%d,'%s','%s',NULL)" %
                               (job_uid,_epochToStr(end_epoch),state))
            else:
                print("  warning: state of '%s' for job with end time" % state)
                cursor.execute("insert into job_states values (%d,'%s','%s',NULL)" %
                               (job_uid,_epochToStr(end_epoch),"done"))
        cursor.close()

    infile.close()
    conn.close()

if __name__ == "__main__":
    writeJobs()
