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

import java.util.*;
import org.apache.log4j.Logger;

public class JobStatisticsCache extends Thread {

    private static Logger logger = Logger.getLogger(JobStatisticsCache.class.getName());

    private static String ALL_JOBS_QUEUE = "all_jobs";

    private boolean keepRunning = true;

    public JobStatisticsCache() {
	start();
    }

    public void run() {
	while (keepRunning) {
	    updateJobStatistics();
	    for(int i=0;(i < 300) && keepRunning;i++) {
		try {
		    Thread.sleep(1*1000);
		} catch (InterruptedException e) {}
	    }
	}
    }

    public void stopRunning() {
	keepRunning = false;
    }

    protected static TreeMap<String,List<JobStatistics>> jobStatistics = new TreeMap<String,List<JobStatistics>>();

    protected static void updateJobStatistics() {
	logger.info("updating job statistics");
	for(String system : GlueDatabase.getSystemNames()) {
	    updateJobStatistics(system);
	}
    }

    protected static void updateJobStatistics(String system) {
	logger.debug("updating job statistics for "+system);

	Date curTime = new Date();

	List<JobStatistics> statsList = new ArrayList<JobStatistics>();
	List<String> queueNames = GlueDatabase.getQueueNames(system);
	queueNames.add(ALL_JOBS_QUEUE);
	for(String queue : queueNames) {
	    logger.debug("creating job statistics for queue "+queue);
	    statsList.add(new JobStatistics(system,queue,curTime,60*60));
	    statsList.add(new JobStatistics(system,queue,curTime,4*60*60));
	    statsList.add(new JobStatistics(system,queue,curTime,24*60*60));
	    statsList.add(new JobStatistics(system,queue,curTime,7*24*60*60));
	}

	int longestHistory = -1;
	for(JobStatistics stats : statsList) {
	    if (stats.historySecs > longestHistory) {
		longestHistory = stats.historySecs;
	    }
	}
	
	logger.debug("  reading started jobs");
	Date earliestTime = new Date(curTime.getTime() - (longestHistory+1) * 1000);
	List<Job> jobs = GlueDatabase.readStartedJobs(system,earliestTime,curTime);
	logger.debug("    found "+jobs.size()+" jobs");
	for(Job job : jobs) {
	    for(JobStatistics stats : statsList) {
		if (!stats.queue.equals(ALL_JOBS_QUEUE)) {
		    if (job.queue == null) {
			continue;
		    }
		    if (!job.queue.equals(stats.queue)) {
			continue;
		    }
		}
		if (!stats.containsTime(job.startTime)) {
		    continue;
		}
		if ((job.submitTime == null) || (job.startTime == null)) { // sanity check
		    continue;
		}

		stats.numStartedJobs++;
		stats.sumProcessors += job.processors;
		stats.sumRequestedRunTime += job.requestedWallTime;
		stats.sumWaitTime += (job.startTime.getTime() - job.submitTime.getTime()) / 1000;
	    }
	}

	logger.debug("  reading ended jobs");
	jobs = GlueDatabase.readEndedJobs(system,earliestTime,curTime);
	logger.debug("    found "+jobs.size()+" jobs");
	for(Job job : jobs) {
	    if (job.endTime == null) { // sanity check
		logger.warn("    end time is null");
		continue;
	    }
	    if (job.startTime == null) { // sanity check
		//logger.debug("    start time is null");
		continue;
	    }
	    for(JobStatistics stats : statsList) {
		if (!stats.queue.equals(ALL_JOBS_QUEUE)) {
		    if (job.queue == null) {
			continue;
		    }
		    if (!job.queue.equals(stats.queue)) {
			continue;
		    }
		}

		if (!stats.containsTime(job.endTime)) {
		    continue;
		}

		stats.numCompletedJobs++;
		stats.sumCompletedProcessors += job.processors;
		stats.sumCompletedRequestedRunTime += job.requestedWallTime;
		stats.sumRunTime += (job.endTime.getTime() - job.startTime.getTime()) / 1000;
	    }
	}

	jobStatistics.put(system,statsList);
	logger.debug("done with "+system);
    }


    public static JobStatistics getJobStatistics(String system, int historySecs) {
	return getJobStatistics(system,ALL_JOBS_QUEUE,historySecs);
    }

    public static JobStatistics getJobStatistics(String system, String queue, int historySecs) {
	if (!jobStatistics.containsKey(system)) {
	    logger.warn("no statistics for system "+system);
	    return new JobStatistics();
	}

	if (queue == null) {
	    queue = ALL_JOBS_QUEUE;
	}
	for(JobStatistics stats : jobStatistics.get(system)) {
	    if (stats.queue.equals(queue)) {
		if (stats.historySecs == historySecs) {
		    return stats;
		}
	    }
	}
	logger.warn("no statistics for "+historySecs+" seconds in queue "+queue+" of system "+system);
	return new JobStatistics();
    }

}
