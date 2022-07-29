/****************************************************************************/
/* Copyright 2015 University of Texas                                       */
/*                                                                          */
/* Licensed under the Apache License, Version 2.0 (the "License");          */
/* you may not use this file except in compliance with the License.         */
/* You may obtain a copy of the License at                                  */
/*                                                                          */
/*     http://www.apache.org/licenses/LICENSE-2.0                           */
/*                                                                          */
/* Unless required by applicable law or agreed to in writing, software      */
/* distributed under the License is distributed on an "AS IS" BASIS,        */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and      */
/* limitations under the License.                                           */
/****************************************************************************/

package karnak.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import org.apache.log4j.Logger;

public class GlueDatabase {

    private static Logger logger = Logger.getLogger(GlueDatabase.class.getName());

    public static int MAX_AGE_MILLISECS = 15 * 60 * 1000;

    static {
	try {
	    Class.forName("com.mysql.jdbc.Driver");
	} catch (Exception e) {
	    logger.error(e.getMessage());
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    private static Connection getConnection() throws SQLException {
	String url = "jdbc:mysql://localhost/glue2";
	String userName="karnak";
	return java.sql.DriverManager.getConnection(url,userName,"");
    }


    public static QueueState readCurrentWaitingJobs(String system) {
	QueueState waitingJobs = new QueueState();
	waitingJobs.system = system;

	QueueState state = readCurrentQueueState(system);
	if (state == null) {
	    return waitingJobs;
	}

	waitingJobs.time = state.time;

	for(String jobId : state.jobIds) {
	    if (state.jobs.containsKey(jobId)) {
		Job job = state.jobs.get(jobId);
		if (job.pendingAtTime(state.time)) {
		    waitingJobs.jobIds.add(jobId);
		    waitingJobs.jobs.put(jobId,job);
		}
	    }
	}

	return waitingJobs;
    }

    // Current is a key word - it means only return info if it isn't too old
    public static QueueState readCurrentQueueState(String system) {
	QueueState state = readLastQueueState(system);
	if (state == null) {
	    logger.warn("no queue state for "+system);
	    return null;
	}
	Date curTime = new Date();
	if (curTime.getTime() - state.time.getTime() > MAX_AGE_MILLISECS) {
	    logger.warn("queue state for "+system+" is too old");
	    logger.debug("  "+curTime+" - "+state.time+" > "+MAX_AGE_MILLISECS);
	    return null;
	}
	return state;
    }

    public static QueueState readLastQueueState(String system) {
	logger.debug("  readLastQueueState for "+system);
	QueueState state = null;
	try {
	    Connection conn = getConnection();
	    Statement stat = conn.createStatement();
	    ResultSet rs = stat.executeQuery("select * from last_queue_states where system='"+system+"'");
	    rs.first();
	    state = new QueueState(rs);
	    rs.close();
	    stat.close();
	    conn.close();
	    //Job.setContext(jobs);
	} catch (Exception e) {
	    logger.error("readCurrentQueueState failed: "+e.getMessage());
	}

	if (state == null) {
	    return state;
	}
	logger.debug("    found queue state for "+state.system+" at time "+state.time);

	readLastJobs(state);
	return state;
    }

    public static Job getCurrentJob(String system, String id) {
	QueueState state = readCurrentQueueState(system);
	if (state == null) {
	    return null;
	}
	return state.jobs.get(id);
    }

    protected static void readLastJobs(QueueState state) {
	state.jobs = readLastJobs(state.system);
    }

    public static Map<String,Job> readLastJobs(String system) {
	logger.debug("  readLastJobs for "+system);
	Map<String,Job> jobs = new TreeMap<String,Job>();
	Date curTime = new Date();
	try {
	    Connection conn = getConnection();
	    Statement stat = conn.createStatement();

	    String command = "select * from last_jobs where system='"+system+"'";
	    ResultSet rs = stat.executeQuery(command);
	    while (rs.next()) {
		Job job = new Job();
		job.fromJob(rs);
		job.updateTime = curTime;
		jobs.put(job.id,job);
	    }
	    rs.close();

	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	return jobs;
    }

    public static Job readJob(String system, String jobId) {
	Job job = new Job();
	try {
	    Connection conn = getConnection();
	    Statement stat = conn.createStatement();

	    String command = "select * from jobs where system='"+system+"'"+" and id='"+jobId+"'"+
		" order by submitTime desc";
	    ResultSet rs = stat.executeQuery(command);
	    if (rs.first()) {
		job.fromJob(rs);
		job.updateTime = new Date();
	    }
	    rs.close();

	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	return job;
    }

    public static void readJobs(QueueState state) {
	state.jobs = readJobs(state.system,state.jobIds);
    }
    
    public static Map<String,Job> readJobs(String system, Collection<String> jobIds) {
	Map<String,Job> jobs = new TreeMap<String,Job>();
	Date curTime = new Date();
	try {
	    Connection conn = getConnection();
	    Statement stat = conn.createStatement();
	    for (String jobId: jobIds) {
		String command = "select * from jobs where system='"+system+"'"+" and id='"+jobId+"'"+
		    " order by submitTime desc";
		ResultSet rs = stat.executeQuery(command);
		if (rs.first()) {
		    Job job = new Job();
		    job.fromJob(rs);
		    job.updateTime = new Date();
		    jobs.put(job.id,job);
		}
		rs.close();
	    }
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	return jobs;
    }

    protected static List<Job> readStartedJobs(String system, Date earliestTime, Date latestTime) {
	return readJobs(system,earliestTime,latestTime,"startTime");
    }

    protected static List<Job> readEndedJobs(String system, Date earliestTime, Date latestTime) {
	return readJobs(system,earliestTime,latestTime,"endTime");
    }

    protected static List<Job> readJobs(String system, Date earliestTime, Date latestTime, String timeRow) {
	Set<Job> jobSet = new TreeSet<Job>(new StartTimeComparator());
	try {
	    Connection conn = getConnection();
	    Statement stat = conn.createStatement();
	    String command = "select * from jobs where system='"+system+"'"+
		" and "+timeRow+" >= '"+WebService.dateToSqlString(earliestTime)+"'"+
		" and "+timeRow+" <= '"+WebService.dateToSqlString(latestTime)+"'";
	    ResultSet rs = stat.executeQuery(command);
	    while (rs.next()) {
		Job job = new Job();
		job.fromJob(rs);
		jobSet.add(job);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	List<Job> jobs = new ArrayList<Job>();
	for(Job job : jobSet) {
	    jobs.add(job);
	}
	return jobs;
    }

    public static Map<String,GlueSystem> readSystems() {
	Map<String,GlueSystem> systems = new TreeMap<String,GlueSystem>();
	try {
	    Connection conn = getConnection();
	    Statement stat = conn.createStatement();
	    Date curTime = new Date();
	    Date cutoffTime = new Date(curTime.getTime() - 60*60*1000);
	    ResultSet rs = stat.executeQuery("select * from system_info where time > '"+
					     WebService.dateToSqlString(cutoffTime) + "'");
	    while (rs.next()) {
		GlueSystem system = new GlueSystem();
		system.name = rs.getString("system");
		system.time = rs.getLong("time");
		system.processors = rs.getInt("processors");
		system.procsPerNode = rs.getInt("procsPerNode");
		systems.put(system.name,system);
	    }
	    rs.close();

	    rs = stat.executeQuery("select * from queue_info");
	    while (rs.next()) {
		if (!rs.getBoolean("isValid")) {
		    continue;
		}
		GlueQueue queue = new GlueQueue();
		queue.name = rs.getString("queue");
		queue.maxProcessors = rs.getInt("maxProcessors");
		queue.maxWallTime = rs.getInt("maxWallTime");
		queue.isDefault = rs.getBoolean("isDefault");
		String systemName = rs.getString("system");
		if (systems.containsKey(systemName)) {
		    //logger.info("queue "+queue.name+" on system "+systemName);
		    systems.get(systemName).queues.put(queue.name,queue);
		} else {
		    //logger.warn("unknown system "+systemName);
		}
	    }
	    rs.close();

	    stat.close();
	    conn.close();
	} catch (Exception e) {
	    logger.error(e.getMessage());
	    e.printStackTrace();
	}
	return systems;
    }

    private static Map<String,GlueSystem> systems = new TreeMap<String,GlueSystem>();
    private static Date systemsUpdateTime = null;

    public static Map<String,GlueSystem> getSystems() {
	Date curTime = new Date();
	if ((systemsUpdateTime == null) || (curTime.getTime() - systemsUpdateTime.getTime() > 15 * 1000)) {
	    systems = readSystems();
	}
	systemsUpdateTime = curTime;

	return systems;
    }

    public static GlueSystem getSystem(String name) {
	return getSystems().get(name);
    }

    public static List<String> getSystemNames() {
	return new ArrayList<String>(getSystems().keySet());
    }

    public static List<String> getQueueNames(String systemName) {
	return new ArrayList<String>(getSystems().get(systemName).queues.keySet());
    }



}
