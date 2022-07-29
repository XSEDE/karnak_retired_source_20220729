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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.*;
import java.util.*;

import org.apache.log4j.Logger;

public class Job {

    private static Logger logger = Logger.getLogger(Job.class.getName());

    public String system = null;
    public String id = null;
    public String name = null;
    public String user = null;
    public String queue = null;
    public String project = null;
    public int processors = -1;
    public int requestedWallTime = -1; // seconds

    public Date submitTime = null;
    public Date startTime = null;
    public Date endTime = null;

    public Date updateTime = null;  // the time this job was read from the db

    public Job() {
    }

    public Job(Job job) {
	system = job.system;
	id = job.id;
	name = job.name;
	user = job.user;
	queue = job.queue;
	project = job.project;
	processors = job.processors;
	requestedWallTime = job.requestedWallTime;
	submitTime = job.submitTime;
	startTime = job.startTime;
	endTime = job.endTime;
	updateTime = job.updateTime;
    }

    public String toString() {
	String str = "Job "+id+" on "+system+"\n";
	str += "  name: "+name+"\n";
	str += "  user: "+user+"\n";
	str += "  queue: "+queue+"\n";
	str += "  project: "+project+"\n";
	str += "  processors: "+processors+"\n";
	str += "  requested wall time: "+requestedWallTime+" secs\n";
	str += "  submit time: "+submitTime+"\n";
	str += "  start time: "+startTime+"\n";
	str += "  end time: "+endTime+"\n";
	return str;
    }

    public void fromJob(ResultSet rs) {
	try {
	    system = rs.getString("system");
	    id = rs.getString("id");
	    name = rs.getString("name");
	    user = rs.getString("user");
	    queue = rs.getString("queue");
	    project = rs.getString("project");
	    processors = rs.getInt("processors");
	    requestedWallTime = rs.getInt("requestedWallTime");

	    // database contains times in UTC, but JDBC is assuming they are in the local time zone
	    //submitTime = rs.getTimestamp("submitTime");
	    //startTime = rs.getTimestamp("startTime");
	    //endTime = rs.getTimestamp("endTime");

	    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");
	    if (rs.getString("submitTime") == null) {
		submitTime = null;
	    } else {
		try {
		    submitTime = format.parse(rs.getString("submitTime")+" UTC");
		} catch (ParseException e) {
		    throw new SQLException(e.getMessage());
		}
	    }
	    if (rs.getString("startTime") == null) {
		startTime = null;
	    } else {
		try {
		    startTime = format.parse(rs.getString("startTime")+" UTC");
		} catch (ParseException e) {
		    throw new SQLException(e.getMessage());
		}
	    }
	    if (rs.getString("endTime") == null) {
		endTime = null;
	    } else {
		try {
		    endTime = format.parse(rs.getString("endTime")+" UTC");
		} catch (ParseException e) {
		    throw new SQLException(e.getMessage());
		}
	    }

	    // state?

	} catch (SQLException e) {
	    logger.error("Job.fromJob: "+e.getMessage());
	    e.printStackTrace();
	}
    }

    public long work() {
	return Long.valueOf(processors) * requestedWallTime;
    }

    public long remainingWork(Date time) {
	if (startTime == null) {
	    return work();
	} else {
	    return work() - processors * (time.getTime() - startTime.getTime()) / 1000;
	}
    }

    public boolean pendingAtTime(Date time) {
	if (submitTime == null) {
	    return false;
	}
	if (time.before(submitTime)) {
	    return false;
	}
	if (startTime == null) {
	    return true;
	}
	if (time.before(startTime)) {
	    return true;
	}
	return false;
    }

    public boolean runningAtTime(Date time) {
	if (startTime == null) {
	    return false;
	}
	if (time.before(startTime)) {
	    return false;
	}
	if (endTime == null) {
	    return true;
	}
	if (time.before(endTime)) {
	    return true;
	}
	return false;
    }

    public boolean doneAtTime(Date time) {
	if (endTime == null) {
	    return false;
	}
	if (time.before(endTime)) {
	    return false;
	}
	return true;
    }

}
