
import datetime

#[karnak@gw27 glue2]$ wc jobs.txt
#  19534298  285114255 2786966894 jobs.txt
#[karnak@gw27 glue2]$ wc queue_states.txt
#   10718242  3079682782 24288066742 queue_states.txt

def _epochToStr(epoch_str):
    epoch = float(epoch_str)
    if epoch <= 0:
        return """\N"""
    dt = datetime.datetime.utcfromtimestamp(epoch)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def doJobs():
    infile = open("/home/karnak/glue2/jobs_orig.txt","r")
    outfile = open("/home/karnak/glue2/jobs.txt","w")
    for line in infile:
        #print(line)
        (system,id,state,name,user,queue,project,processors,req_wall_time,
         submit_epoch,start_epoch,end_epoch) = line.split("\t")

        dtline = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % \
                 (system,id,state,name,user,queue,project,processors,req_wall_time,
                  _epochToStr(submit_epoch),_epochToStr(start_epoch),_epochToStr(end_epoch))
        outfile.write(dtline)
        outfile.write("\n")
    infile.close()
    outfile.close()

def doQueues():
    infile = open("/home/karnak/glue2/queue_states_orig.txt","r")
    outfile = open("/home/karnak/glue2/queue_states.txt","w")
    for line in infile:
        #print(line)
        (system,epoch,jobs) = line.split("\t")
        dtline = "%s\t%s\t%s" % (system,_epochToStr(epoch),jobs)
        outfile.write(dtline)
        #outfile.write("\n")
    infile.close()
    outfile.close()

if __name__ == "__main__":
    doJobs()
    doQueues()
