
package karnak.service.predict;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.*;
import java.util.*;

import karnak.learn.*;
import karnak.service.*;

public abstract class WaitTimeExperience implements Experience {

    public String system = null;
    public String id = null;
    public String name = null;
    public String user = null;
    public String queue = null;
    public String project = null;
    public int processors = -1;
    public int requestedWallTime = -1; // seconds

    public long time = -1;

    // the below are all at 'time'
    public int count = -1;             // running or waiting ahead of this job
    public long work = -1;             // running or waiting ahead of this job in processor seconds
    public int userCount = -1;         // running or waiting ahead of this job by this user
    public long userWork = -1;         // running or waiting ahead of this job by this user in processor seconds
    public boolean jobsRunning = true; // some jobs are running

    public long simulatedStartTime = -1;

    public long startTime = -1;

    public WaitTimeExperience() {
    }

    public WaitTimeExperience(Job job) {
	system = job.system;
	id = job.id;
	name = job.name;
	user = job.user;
	queue = job.queue;
	project = job.project;
	processors = job.processors;
	requestedWallTime = job.requestedWallTime;

	startTime = job.getStartTime();
    }

    public WaitTimeExperience(WaitTimeExperience exp) {
	system = exp.system;
	id = exp.id;
	name = exp.name;
	user = exp.user;
	queue = exp.queue;
	project = exp.project;
	processors = exp.processors;
	requestedWallTime = exp.requestedWallTime;

        time = exp.time;

        count = exp.count;
        work = exp.work;
        userCount = exp.userCount;
        userWork = exp.userWork;

        simulatedStartTime = exp.simulatedStartTime;

        startTime = exp.startTime;
    }

    public WaitTimeExperience(ResultSet rs) {
	fromSql(rs);
    }

    public String toString() {
        return toString("");
    }

    public String toString(String indent) {
	String str = indent+"Job "+id+" on "+system+"\n";
	str += indent+"  name: "+name+"\n";
	str += indent+"  user: "+user+"\n";
	str += indent+"  queue: "+queue+"\n";
	str += indent+"  project: "+project+"\n";
	str += indent+"  processors: "+processors+"\n";
	str += indent+"  requested wall time: "+requestedWallTime+" secs\n";
	if (time != -1) {
	    str += indent+"  time: "+Service.epochToString(time)+"\n";
	}

	if (count != -1) {
	    str += indent+"  jobs ahead : "+count+"\n";
	}
	if (work != -1) {
	    str += indent+"  work ahead: "+work+"\n";
	}
	if (userCount != -1) {
	    str += indent+"  user's jobs ahead : "+userCount+"\n";
	}
	if (userWork != -1) {
	    str += indent+"  user's work ahead: "+userWork+"\n";
	}
	if (jobsRunning) {
	    str += indent+"  jobs are running\n";
	} else {
	    str += indent+"  jobs are not running\n";
	}
	if (simulatedStartTime != -1) {
	    str += indent+"  simulated start time: "+Service.epochToString(simulatedStartTime)+"\n";
	}
	if (startTime != -1) {
	    str += indent+"  start time: "+Service.epochToString(startTime)+"\n";
	}

	return str;
    }

    public static String getSqlDef() {
        return "system varchar(80), id varchar(80), name text, user text, queue text, project text, " +
            "processors integer, requestedWallTime integer, time real, " +
            "count integer, work real, userCount integer, userWork real, jobsRunning bit(1), " + 
	    "simulatedStartTime real, startTime real";
    }

    public void fromSql(ResultSet rs) {
	try {
	    system = rs.getString("system");
	    id = rs.getString("id");
	    name = rs.getString("name");
	    if (name.equals("")) {
		name = null;
	    }
	    user = rs.getString("user");
	    if (user.equals("")) {
		user = null;
	    }
	    queue = rs.getString("queue");
	    if (queue.equals("")) {
		queue = null;
	    }
	    project = rs.getString("project");
	    if (project.equals("")) {
		project = null;
	    }
	    processors = rs.getInt("processors");
	    requestedWallTime = rs.getInt("requestedWallTime");
	    time = rs.getLong("time");
	    count = rs.getInt("count");
	    work = rs.getLong("work");
	    userCount = rs.getInt("userCount");
	    userWork = rs.getLong("userWork");
	    jobsRunning = rs.getBoolean("jobsRunning");
	    simulatedStartTime = rs.getLong("simulatedStartTime");
	    startTime = rs.getLong("startTime");
	} catch (SQLException e) {
	    System.out.println(e.getMessage());
	    e.printStackTrace();
	}
    }

    public String toSql() {
        String sqlStr = "'" + system + "', " +
            "'" + id + "', " +
            "'" + name + "', " +
            "'" + user + "', " +
            "'" + queue + "', " +
            "'" + project + "', " +
            processors + ", " +
            requestedWallTime + ", " +
            time + ", " +
	    count + ", " +
            work + ", " +
            userCount + ", " +
            userWork + ", ";
        if (jobsRunning) {
	    sqlStr += "1, ";
	} else {
	    sqlStr += "0, ";
	}
	sqlStr += simulatedStartTime + ", " + startTime;

	return sqlStr;
    }

    public void setContext(QueueState queue) {
	time = queue.time;

	for(Job job : queue.jobs) {
	    if (job.runningAtTime(time)) {
		jobsRunning = true;
		break;
	    }
	}

	count = 0;
	work = 0;
	userCount = 0;
	userWork = 0;
	for(Job job : queue.jobs) {
	    if ((id != null) && id.equals(job.id)) { // stop if we find this job (id is null for unsubmitted job)
		break;
	    }
	    if (job.doneAtTime(time)) {     // ignore jobs that are done
		continue;
	    }
	    if (job.heldAtTime(time)) {     // ignore held jobs
		continue;
	    }

	    long runningTime = 0;
	    if (job.runningAtTime(time)) {
		runningTime = time - job.getStartTime();
		if (runningTime > job.requestedWallTime) {
		    runningTime = job.requestedWallTime;
		}
	    }

	    count++;
	    work += job.processors * (job.requestedWallTime - runningTime);

	    if ((user == null) || (job.user == null) || !user.equals(job.user)) {
		continue;
	    }
	    userCount++;
	    userWork += job.processors * (job.requestedWallTime - runningTime);
	}
    }

    public abstract long getTime();

}