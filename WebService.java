
package karnak.service;

import com.sun.grizzly.http.SelectorThread;
import com.sun.jersey.api.container.grizzly.GrizzlyWebContainerFactory;
import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.text.*;
import java.util.*;
import javax.ws.rs.core.UriBuilder;
import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.service.*;
import karnak.service.predict.*;
import karnak.service.task.*;

public class WebService extends Service {

    private static Logger logger = Logger.getLogger(WebService.class.getName());

    private static String ALL_JOBS_QUEUE = "all_jobs";

    private SubmittedStartTimePredictor submittedPredictor = null;
    private UnsubmittedStartTimePredictor unsubmittedPredictor = null;

    public WebService() {
	super();

	taskThread.add(new JobStatisticsTask());

	submittedPredictor = new SubmittedStartTimePredictor(this);
	unsubmittedPredictor = new UnsubmittedStartTimePredictor(this);
    }

    public void stopRunning() {
	super.stopRunning();
    }

    public JobSummary getJobSummary(String system) {
	return getJobSummary(system,null);
    }

    public JobSummary getJobSummary(String system, String queue) {
	logger.debug("getJobSummary for "+queue+" on "+system);

	JobSummary summary = new JobSummary(system,queue);

	QueueState queueState = getCurrentQueueState(system);
	if (queueState == null) {
	    return summary;
	}

	int numRunning = 0;
	int numWaiting = 0;
	int usedProcessors = 0;
	for(Job job : queueState.jobs) {
	    if (queue != null) {
		if (job.queue != null) {
		    if (!queue.equals(job.queue)) {
			continue;
		    }
		}
	    }
	    if (job.runningAtTime(queueState.time)) {
		numRunning++;
		usedProcessors += job.processors;
	    }
	    if (job.pendingAtTime(queueState.time)) {
		numWaiting++;
	    }
	}

	summary.setState(numRunning,numWaiting,usedProcessors);
	return summary;
    }

    class JobStatisticsTask extends PeriodicTask {
	public JobStatisticsTask() {
	    super(10*60);
	}

	public void runTask() {
	    updateJobStatistics();
	}
    }

    protected TreeMap<String,List<JobStatistics>> jobStatistics = new TreeMap<String,List<JobStatistics>>();

    protected void updateJobStatistics() {
	logger.info("updating job statistics");
	logger.info("  "+getSystemNames().size()+" systems");
	for(String system : getSystemNames()) {
	    updateJobStatistics(system);
	}
    }

    protected void updateJobStatistics(String system) {
	//logger.debug("updating job statistics for "+system);
	logger.info("updating job statistics for "+system);

	long curTime = currentEpoch();

	List<JobStatistics> statsList = new ArrayList<JobStatistics>();
	List<String> queueNames = getQueueNames(system);
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
	
	logger.info("reading started jobs");
	List<Job> jobs = readStartedJobs(system,curTime-longestHistory+1,curTime);
	logger.info("  found "+jobs.size()+" jobs");
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
		if (!stats.containsTime(job.getStartTime())) {
		    continue;
		}
		if ((job.getSubmitTime() < 0) || (job.getStartTime() < 0)) { // sanity check
		    continue;
		}

		stats.numStartedJobs++;
		stats.sumProcessors += job.processors;
		stats.sumRequestedRunTime += job.requestedWallTime;
		stats.sumWaitTime += job.getStartTime() - job.getSubmitTime();
	    }
	}

	logger.info("reading ended jobs");
	jobs = readEndedJobs(system,curTime-longestHistory+1,curTime);
	logger.info("  found "+jobs.size()+" jobs");
	for(Job job : jobs) {
	    if (job.getEndTime() < 0) { // sanity check
		logger.warn("    end time < 0");
		continue;
	    }
	    if (job.getStartTime() < 0) { // sanity check
		logger.debug("    start time < 0");
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

		if (!stats.containsTime(job.getEndTime())) {
		    continue;
		}

		stats.numCompletedJobs++;
		stats.sumCompletedProcessors += job.processors;
		stats.sumCompletedRequestedRunTime += job.requestedWallTime;
		stats.sumRunTime += job.getEndTime() - job.getStartTime();
	    }
	}

	jobStatistics.put(system,statsList);
    }


    public JobStatistics getJobStatistics(String system, int historySecs) {
	return getJobStatistics(system,ALL_JOBS_QUEUE,historySecs);
    }

    public JobStatistics getJobStatistics(String system, String queue, int historySecs) {
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


    public SubmittedWaitTimePrediction getStartPrediction(Job job) throws PredictException {
	SubmittedWaitTimePrediction query = new SubmittedWaitTimePrediction(job);

	return submittedPredictor.predict(query);
    }

    public UnsubmittedWaitTimePrediction getStartPrediction(String system,
                                                            String queue,
                                                            int processingCores,
                                                            int reqRunTime) throws PredictException {
	UnsubmittedWaitTimePrediction query = new UnsubmittedWaitTimePrediction();
	query.system = system;
	query.queue = queue;
	query.processors = processingCores;
	query.requestedWallTime = reqRunTime;

	try {
	    return unsubmittedPredictor.predict(query);
	} catch (PredictException e) {
	    logger.error(e.getMessage());
	    throw e;
	}
    }



    private static int getPort(int defaultPort) {
        String port = System.getenv("JERSEY_HTTP_PORT");
        if (null != port) {
            try {
                return Integer.parseInt(port);
            } catch (NumberFormatException e) {
            }
        }
        return defaultPort;        
    } 
    
    private static URI getBaseURI() {
        //return UriBuilder.fromUri("http://localhost/").port(getPort(9080)).build();
        return UriBuilder.fromUri("http://localhost/").port(getPort(8080)).build();
    }

    public static final URI BASE_URI = getBaseURI();

    protected static SelectorThread startServer() throws IOException {
        final Map<String, String> initParams = new HashMap<String, String>();

        initParams.put("com.sun.jersey.config.property.packages","karnak.service.web.resources");

        logger.info("Starting grizzly...");
        SelectorThread threadSelector = GrizzlyWebContainerFactory.create(BASE_URI, initParams);     
        return threadSelector;
    }

    public static WebService service = null;
    
    public static void main(String[] args) throws IOException {

	logger.info("creating WebService");
	service = new WebService();

        SelectorThread threadSelector = startServer();

	logger.info("service is running");
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nTry out %stest\nHit enter to stop it...",
                BASE_URI, BASE_URI));
        System.in.read();

        threadSelector.stopEndpoint();
	service.stopRunning();
    }    

}
