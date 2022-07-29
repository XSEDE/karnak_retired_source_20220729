
package karnak.service.predict;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;

import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.learn.trace.*;
import karnak.service.*;

public class GenerateExperiences extends Service {

    private static Logger logger = Logger.getLogger(GenerateExperiences.class.getName());

    public static void main(String[] args) {
	if (!((args.length == 0) || (args.length == 4))) {
	    System.err.println("usage: GenerateExperiences [<run/unsub/sub/all> <system> <start> <end>]");
	    System.err.println("         date/time format is year-month-day (time is assumed to be 00:00:00 GMT)");
	    System.exit(1);
	}

	GenerateExperiences gen = new GenerateExperiences();
	//gen.createTables()
	if (args.length == 0) {
	    gen.run();
	} else {
	    long start = dateToEpoch(args[2]);
	    long end = dateToEpoch(args[3]);
	    if (args[0].equals("run") ||  args[0].equals("all")) {
		gen.generateRunTimeExperiences(args[1],start,end);
	    }
	    if (args[0].equals("unsub") ||  args[0].equals("all")) {
		gen.generateUnsubmittedExperiences(args[1],start,end);
	    }
	    if (args[0].equals("sub") ||  args[0].equals("all")) {
		/* if it is taking too long to do scheduling simulations
		List<Job> jobsToGenerate = gen.readStartedJobs(args[1],start,end);
		logger.info("loaded "+jobsToGenerate.size()+" jobs");
		gen.generateSubmittedExperiences(jobsToGenerate);
		gen.generateSimulatedStartTimes(jobsToGenerate);
		*/
		/* if it isn't */
		gen.generateSubmittedExperiences(args[1],start,end);
	    }
	}

	gen.stopRunning();
    }

    public GenerateExperiences() {
	super();
    }

    protected void createTables() {
	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();

	    stat.execute("create table if not exists runtime_experiences ("+RunTimeExperience.getSqlDef()+")");
	    stat.execute("create table if not exists unsubmitted_experiences ("+
                         UnsubmittedWaitTimeExperience.getSqlDef()+")");
	    stat.execute("create table if not exists submitted_experiences ("+
                         SubmittedWaitTimeExperience.getSqlDef()+")");

	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }


    protected void run() {
	logger.info("starting GenerateExperiences service");
	//System.out.close();
	//System.err.close();

	while(true) {
	    long startTime = currentEpoch();
	    for(String system : getSystemNames()) {
		logger.info(">>>>> updating system "+system+" <<<<<");
		//QueueState queueState = readLatestQueueState(system);
		QueueState queueState = readLastQueueState(system);

		long latest = getLatestRunTimeExperienceTime(system);
		List<Job> jobs = readEndedJobs(system,latest+1,queueState.time);
		logger.info("generating run time experiences for "+jobs.size()+" jobs");
		generateRunTimeExperiencesSaveTime(jobs);

		latest = getLatestWaitTimeExperienceTime(system);
		jobs = readStartedJobs(system,latest+1,queueState.time);
		logger.info("wait time experiences for "+jobs.size()+" jobs");
		generateUnsubmittedExperiencesSaveTime(jobs);
		generateSubmittedExperiences(jobs);
	    }
	    long curTime = currentEpoch();
	    try {
		long sleepSecs = 2*60 - (curTime-startTime);
		if (sleepSecs > 0) {
		    Thread.sleep(sleepSecs*1000);
		}
	    } catch (InterruptedException e) {}
	}
    }

    protected TreeMap<String,Long> latestRunTimeExpTime = new TreeMap<String,Long>();

    protected long getLatestRunTimeExperienceTime(String system) {
	if (latestRunTimeExpTime.containsKey(system)) {
	    return latestRunTimeExpTime.get(system);
	}
	long latest = readLatestRunTimeExperienceTime(system);
	latestRunTimeExpTime.put(system,latest);
	return latest;
    }

    protected long readLatestRunTimeExperienceTime(String system) {
	long latest = -1;
	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();
	    String command = "select max(endTime) from runtime_experiences where system='"+system+"'";
	    ResultSet rs = stat.executeQuery(command);
	    if (rs.first()) {
		latest = rs.getLong(1);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	return latest;
    }

    public void generateRunTimeExperiencesSaveTime(List<Job> jobs) {
	generateRunTimeExperiences(jobs);
	long latestEndTime = -1;
	for(Job job : jobs) {
	    if (job.getEndTime() > latestEndTime) {
		latestEndTime = job.getEndTime();
	    }
	}
	if (latestEndTime > 0) {
	    latestRunTimeExpTime.put(jobs.get(0).system,latestEndTime);
	}
    }


    protected TreeMap<String,Long> latestWaitTimeExpTime = new TreeMap<String,Long>();

    protected long getLatestWaitTimeExperienceTime(String system) {
	if (latestWaitTimeExpTime.containsKey(system)) {
	    return latestWaitTimeExpTime.get(system);
	}
	long latest = readLatestWaitTimeExperienceTime(system);
	latestWaitTimeExpTime.put(system,latest);
	return latest;
    }

    protected long readLatestWaitTimeExperienceTime(String system) {
	long latest = -1;
	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();
	    String command = "select max(startTime) from unsubmitted_experiences where system='"+system+"'";
	    ResultSet rs = stat.executeQuery(command);
	    if (rs.first()) {
		latest = rs.getLong(1);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	return latest;
    }

    public void generateUnsubmittedExperiencesSaveTime(List<Job> jobs) {
	generateUnsubmittedExperiences(jobs);
	long latestStartTime = -1;
	for(Job job : jobs) {
	    if (job.getStartTime() > latestStartTime) {
		latestStartTime = job.getStartTime();
	    }
	}
	if (latestStartTime > 0) {
	    latestWaitTimeExpTime.put(jobs.get(0).system,latestStartTime);
	}
    }


    protected void generateRunTimeExperiences(String system, long start, long end) {
	logger.info("generating run time experiences");
	List<Job> jobsToGenerate = readStartedJobs(system,start,end-1);
	logger.info("  loaded "+jobsToGenerate.size()+" jobs");
	generateRunTimeExperiences(jobsToGenerate);
    }

    protected void generateRunTimeExperiences(List<Job> jobsToGenerate) {
	for(Job job : jobsToGenerate) {
	    RunTimeExperience exp = new RunTimeExperience(job);
	    if (exp != null) {
		//logger.info(exp);
		writeRunTimeExperience(exp);
	    }
	}
    }

    protected void generateUnsubmittedExperiences(String system, long start, long end) {
	logger.info("generating unsubmitted experiences");
	List<Job> jobsToGenerate = readStartedJobs(system,start,end-1);
	logger.info("  loaded "+jobsToGenerate.size()+" jobs");
	generateUnsubmittedExperiences(jobsToGenerate);
    }

    protected void generateUnsubmittedExperiences(List<Job> jobsToGenerate) {
	for(Job job : jobsToGenerate) {
	    logger.info("job that started at "+epochToString(job.getStartTime())+" on "+job.system);
	    QueueState queue = readQueueStateBefore(job.system,job.getSubmitTime());
	    if (queue == null) {
		logger.warn("  didn't find queue state for "+epochToString(job.getSubmitTime()));
		continue;
	    }

	    logger.info("  getting information on "+queue.jobIds.size()+" jobs...");
	    getJobs(queue);

	    if (queue.jobs.size() < queue.jobIds.size()) {
		logger.warn("found "+queue.jobs.size()+" of "+queue.jobIds.size()+" jobs");
		if (queue.jobs.size() < queue.jobIds.size() * 0.95) {
		    logger.warn("  not generating unsubmitted experience");
		    continue;
		}
	    }

	    queue.jobs.add(job);

	    UnsubmittedWaitTimeExperience exp = new UnsubmittedWaitTimeExperience(job);
	    exp.setContext(queue);
	    //logger.info(exp);
	    writeUnsubmittedExperience(exp);
	}
    }


    protected void generateSubmittedExperiences(String system, long start, long end) {
	List<Job> jobsToGenerate = readStartedJobs(system,start,end-1);
	logger.info("loaded "+jobsToGenerate.size()+" jobs");
	generateSubmittedExperiences(jobsToGenerate);
    }

    protected void generateSubmittedExperiences(List<Job> jobsToGenerate) {
	// now generate experiences for the jobs
	int numWritten = 0;
	int numQueueStates = 0;
	for(Job job : jobsToGenerate) {
	    logger.info("job submitted at "+epochToString(job.getSubmitTime())+" that started at "+
			epochToString(job.getStartTime()));
	    QueueState queue = getQueueState(job);
	    if (queue == null) {
		logger.warn("didn't find queue state for job "+job.id);
		continue;
	    }
	    numQueueStates += 1;

	    getJobs(queue);

	    if (queue.jobs.size() < queue.jobIds.size()) {
		logger.warn("found "+queue.jobs.size()+" of "+queue.jobIds.size()+" jobs");
		if (queue.jobs.size() < queue.jobIds.size() * 0.95) {
		    logger.warn("  not generating unsubmitted experience");
		    continue;
		}
	    }

	    for(Job queueJob : queue.jobs) {
		if (queueJob.id.equals(job.id)) {
		    SubmittedWaitTimeExperience exp = new SubmittedWaitTimeExperience(queueJob);
		    exp.setContext(queue);
		    exp.startTime = job.getStartTime();
		    break;
		}
	    }
	}
	logger.info("wrote "+numWritten+" experiences for "+numQueueStates+" queue states");
    }

    protected QueueState readQueueStateBefore(String system, long time) {
	QueueState queueState = null;
	try {
	    Connection conn = getGlueConnection();
	    Statement stat = conn.createStatement();
	    String command = "select * from queue_states where system='"+system+"'"+
		" and time<"+time+
		" and time>="+(time-10*60)+
		" order by time desc;";
	    ResultSet rs = stat.executeQuery(command);
	    if (rs.next()) {
		queueState = new QueueState(rs);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	return queueState;
    }

    protected List<QueueState> readQueueStates(String system, long start, long end) {
	List<QueueState> states = new ArrayList<QueueState>();
	try {
	    Connection conn = getGlueConnection();
	    Statement stat = conn.createStatement();
	    String command = "select * from queue_states where system='"+system+"'"+
		" and time>="+start+
		" and time<="+end+
		" order by time asc";
	    ResultSet rs = stat.executeQuery(command);
	    while (rs.next()) {
		states.add(new QueueState(rs));
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	return states;
    }

    protected QueueState getQueueState(Job job) {
	// just doing a single experience per job - right after the job is submitted

	QueueState qs = null;
	try {
	    Connection conn = getGlueConnection();
	    Statement stat = conn.createStatement();
	    String command = "select * from queue_states where system='"+job.system+"'"+
		" and time>="+job.getSubmitTime()+
		" and time<"+job.getStartTime();
	    command += " order by time asc;";
	    ResultSet rs = stat.executeQuery(command);
	    while (rs.next()) {
		qs = new QueueState(rs);
		if (qs.jobIds.contains(job.id)) {
		    break;
		}
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	return qs;
    }

    protected TreeMap<String,TreeMap<String,Job>> jobCache = new TreeMap<String,TreeMap<String,Job>>();

    /*
    public Job getJob(String system, String jobId) {
	TreeSet<String> jobIds = new TreeSet<String>();
	jobIds.add(jobId);
	TreeMap<String,Job> jobs = getJobs(system,jobIds);
	return jobs.get(jobId);
    }
    */

    protected void pruneCache(String system, long time) {
	if (!jobCache.containsKey(system)) {
	    return;
	}
	for(String jobId : jobCache.get(system).keySet()) {
	    long endTime = jobCache.get(system).get(jobId).getEndTime();
	    if ((endTime > 0) && (endTime < time)) {
		jobCache.remove(jobId);
	    }
	}
    }

    protected void getJobs(QueueState queue) {
	if (!jobCache.containsKey(queue.system)) {
	    jobCache.put(queue.system,new TreeMap<String,Job>());
	}
	pruneCache(queue.system,queue.time);

	TreeSet<String> jobsToRead = new TreeSet<String>();
	for(String jobId : queue.jobIds) {
	    Job job = jobCache.get(queue.system).get(jobId);
	    if ((job == null) || (job.updateTime < queue.time)) {
		jobsToRead.add(jobId);
	    }
	}

	logger.info("    reading "+jobsToRead.size()+" jobs...");
	TreeMap<String,Job> readJobs = readJobs(queue.system,jobsToRead);
	for(Job job : readJobs.values()) {
	    jobCache.get(queue.system).put(job.id,job);
	}

	queue.jobs.clear();
	for(String jobId : queue.jobIds) {
	    if (jobCache.get(queue.system).containsKey(jobId)) {
		queue.jobs.add(jobCache.get(queue.system).get(jobId));
	    }
	}
    }

    /*
    protected QueueState getJobs(QueueState queue) {
	queue.jobs = new ArrayList<Job>(); // just in case

	TreeMap<String,Job> jobs = getJobs(queue.system,queue.jobIds);
	for (String id : queue.jobIds) {
	    if (jobs.containsKey(id)) {
		queue.jobs.add(jobs.get(id));
	    } else {
		//logger.warn("  didn't find job "+id+" on "+system);
	    }
	}

	return queue;
    }

    public TreeMap<String,Job> getJobs(String system, List<String> jobIds) {
	TreeSet<String> jobIdSet = new TreeSet<String>();
	for(String jobId : jobIds) {
	    jobIdSet.add(jobId);
	}
	return getJobs(system,jobIdSet);
    }

    public TreeMap<String,Job> getJobs(String system, TreeSet<String> jobIds) {
	TreeMap<String,Job> jobs = new TreeMap<String,Job>();
	if (!jobCache.containsKey(system)) {
	    jobCache.put(system,new TreeMap<String,Job>());
	}
	TreeSet<String> allJobIds = new TreeSet<String>(jobIds);

	long curTime = currentEpoch();
	for(String jobId : allJobIds) {
	    if (jobCache.get(system).containsKey(jobId)) {
		Job job = jobCache.get(system).get(jobId);
		if (job != null) {
		    if (job.doneAtTime(curTime)) { // if the job isn't done, safer to check the db for an update
			jobs.put(jobId,jobCache.get(system).get(jobId));
			jobIds.remove(jobId);
		    }
		}
	    }
	}

	logger.info("    reading "+jobIds.size()+" jobs...");
	TreeMap<String,Job> readJobIds = readJobs(system,jobIds);
	for(Job job : readJobIds.values()) {
	    jobs.put(job.id,job);
	    jobCache.get(system).put(job.id,job);
	}
	for(String jobId : jobIds) {
	    if (!readJobIds.containsKey(jobId)) {
		jobCache.get(system).put(jobId,null);
	    }
	}

	return jobs;
    }
    */

    protected void writeRunTimeExperience(RunTimeExperience exp) {
	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();
	    String command = "insert into runtime_experiences values ("+exp.toSql()+")";
	    stat.executeUpdate(command);
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    logger.error("writeRunTimeExperience: "+e.getMessage());
	}
    }

    protected void writeUnsubmittedExperience(UnsubmittedWaitTimeExperience exp) {
	writeExperience("unsubmitted_experiences",exp);
    }

    protected void writeSubmittedExperience(SubmittedWaitTimeExperience exp) {
	writeExperience("submitted_experiences",exp);
    }

    protected void writeExperience(String tableName, WaitTimeExperience exp) {
	//System.out.println("writing "+job);
	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();
	    String command = "insert into "+tableName+" values ("+exp.toSql()+")";
	    //System.out.println(command);
	    stat.executeUpdate(command);
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    logger.error("writeExperience: "+e.getMessage());
	    //e.printStackTrace();
	}
    }

}
