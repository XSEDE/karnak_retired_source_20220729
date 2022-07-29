
package karnak.service.resources;

import java.util.*;
import java.text.SimpleDateFormat;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

import org.apache.log4j.Logger;

import karnak.KarnakException;
import karnak.service.predict.SubmittedPredictor;
import karnak.service.*;
import karnak.service.predict.*;

/* 08/24/2016
   serve both starttime and waittime requests
*/
@Path("/karnak/v2/{requesttype}/system/{system}/job")
public class SubmittedJobResource {

    private static Logger logger = Logger.getLogger(SubmittedJobResource.class.getName());
    
    @Path("waiting.txt")
    @GET 
    @Produces("text/plain")
    public String getSystemText(@PathParam("system") String system) {
	return getWaitingJobsText(system);
    }
    
    @Path("waiting.html")
    @GET 
    @Produces("text/html")
    public String getSystemHtml(@PathParam("system") String system,
	                        @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getWaitingJobsHtml(system,true,true);
    }
    
    @Path("waiting.xml")
    @GET 
    @Produces("text/xml")
    public String getSystemXml(@PathParam("system") String system) {
	return getWaitingJobsXml(system);
    }

    @Path("waiting.json")
    @GET 
    @Produces("application/json")
    public String getSystemJson(@PathParam("system") String system) {
	return getWaitingJobsJson(system);
    }


    static String getWaitingJobsText(String system) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(system)) {
	    return "Unknown system "+system+"\n";
	}

	String text = "";
	text = SystemsResource.textHeading("Waiting Jobs on "+system,70);

	QueueState waiting = GlueDatabase.readCurrentWaitingJobs(system);

	Table table = new Table("Waiting Jobs on "+waiting.system);
	table.setHeader("Job\nIdentifier",
			"Submit Time\n("+WebService.getLocalTimeZoneString()+")",
			"\nProcessors",
			"Requested Wall Time\n(hours:minutes:seconds)");

	for(String jobId : waiting.jobIds) {
	    Job job = waiting.jobs.get(jobId);
	    table.addRow(job.id,
			 WebService.dateToLocalString(job.submitTime),
			 String.valueOf(job.processors),
			 WebService.hms(job.requestedWallTime));
	}

        return table.getText();
    }

    static String getWaitingJobsHtml(String system, boolean waitTimePrediction, boolean startTimePrediction) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(system)) {
	    return "<p>Unknown system "+system+"</p>\n";
	}

	QueueState waiting = GlueDatabase.readCurrentWaitingJobs(system);

	Table table = new Table("Waiting Jobs on "+waiting.system);
	if (waitTimePrediction && startTimePrediction) {
	    table.setHeader("Job Identifier",
			    "Submit Time\n("+WebService.getLocalTimeZoneString()+")",
			    "Processors",
			    "Requested Wall Time\n(hours:minutes:seconds)",
			    "Predictions");
	    table.setDataJustification(Table.JUSTIFICATION_CENTER,
				       Table.JUSTIFICATION_CENTER,
				       Table.JUSTIFICATION_RIGHT,
				       Table.JUSTIFICATION_CENTER,
				       Table.JUSTIFICATION_CENTER);
	} else {
	    table.setHeader("Job Identifier",
			    "Submit Time\n("+WebService.getLocalTimeZoneString()+")",
			    "Processors",
			    "Requested Wall Time\n(hours:minutes:seconds)");
	    table.setDataJustification(Table.JUSTIFICATION_CENTER,
				       Table.JUSTIFICATION_CENTER,
				       Table.JUSTIFICATION_RIGHT,
				       Table.JUSTIFICATION_CENTER);
	}

	for(String jobId : waiting.jobIds) {
	    Job job = waiting.jobs.get(jobId);
	    if (waitTimePrediction && startTimePrediction) {
		String pred = "<a href='/karnak/v2/waittime/system/"+job.system+"/job/"+job.id+
		    "/prediction/waittime.html'>wait time</a>" + 
		    "<a href='/karnak/v2/starttime/system/"+job.system+"/job/"+job.id+
		    "/prediction/starttime.html'>start time</a>";
		table.addRow(job.id,
			     WebService.dateToLocalString(job.submitTime),
			     String.valueOf(job.processors),
			     WebService.hms(job.requestedWallTime),
			     pred);
	    } else {
		String jobIdCell = "";
		if (waitTimePrediction && !startTimePrediction) {
		    jobIdCell = "<a href='/karnak/v2/waittime/system/"+job.system+"/job/"+job.id+"/prediction/waittime.html'>"+
			job.id+"</a>";
		} else if (!waitTimePrediction && startTimePrediction) {
		    jobIdCell = "<a href='/karnak/v2/starttime/system/"+job.system+"/job/"+job.id+"/prediction/starttime.html'>"+
			job.id+"</a>";
		}
		table.addRow(jobIdCell,
			     WebService.dateToLocalString(job.submitTime),
			     String.valueOf(job.processors),
			     WebService.hms(job.requestedWallTime));
	    }
	}

        return table.getHtml();
    }

    static String getWaitingJobsXml(String system) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(system)) {
	    return "<Error xmlns='"+SystemsResource.namespace+"'>Unknown system "+system+"</Error>\n";
	}
	QueueState waiting = GlueDatabase.readCurrentWaitingJobs(system);
	TreeNode root = getWaitingJobsTree(waiting);
	return root.getXml();
    }

    static String getWaitingJobsJson(String system) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(system)) {
	    return "{ \"Error\" : \"Unknown system "+system+"\" }\n";
	}
	QueueState waiting = GlueDatabase.readCurrentWaitingJobs(system);
	TreeNode root = getWaitingJobsTree(waiting);
	return root.getJson();
    }

    static TreeNode getWaitingJobsTree(QueueState waiting) {
	TreeNode root = new TreeNode("System",null,SystemsResource.namespace);
	root.addChild(new TreeNode("Name",waiting.system));
	TreeNode jobsNode = new TreeNode("WaitingJobs"); root.addChild(jobsNode);
	jobsNode.addAttribute(new TreeNode("time",WebService.dateToString(waiting.time)));
	for(String jobId : waiting.jobIds) {
	    Job job = waiting.jobs.get(jobId);
	    TreeNode jobNode = new TreeNode("Job"); jobsNode.addChild(jobNode);
	    jobNode.addChild(new TreeNode("Identifier",job.id));
	    jobNode.addChild(new TreeNode("SubmitTime",WebService.dateToString(job.submitTime)));
	    jobNode.addChild(new TreeNode("Processors",job.processors));
	    TreeNode reqWallTime = new TreeNode("RequestedWallTime",job.requestedWallTime/60.0);
	    reqWallTime.addAttribute(new TreeNode("units","minutes"));
	    jobNode.addChild(reqWallTime);
	}
	return root;
    }

    /*
    static String getWaitingJobsXml(String system) {
	List<String> systems = GlueDatabase.getSystemNames();

	if (!systems.contains(system)) {
	    return "<Error xmlns='"+SystemsResource.namespace+"'>Unknown system "+system+"</Error>\n";
	}

	String text = "<System xmlns='"+SystemsResource.namespace+"'>\n";
	text = text + "  <Name>"+system+"</Name>\n";

	QueueState waiting = GlueDatabase.getCurrentWaitingJobs(system);
	text = text + "  <WaitingJobs time='"+WebService.dateToString(waiting.time)+"'>\n";
	for (Job job : waiting.jobs) {
	    text = text + "    <Job>\n";
	    text = text + "      <Identifier>"+job.id+"</Identifier>\n";
	    text = text + "      <SubmitTime>"+WebService.dateToString(job.submitTime)+"</SubmitTime>\n";
	    text = text + "      <Processors>"+job.processors+"</Processors>\n";
	    text = text + "      <RequestedWallTime units='seconds'>"+job.requestedWallTime+"</RequestedWallTime>\n";
	    text = text + "    </Job>\n";
	}
	text = text + "  </WaitingJobs>\n";
	text = text + "</System>\n";

        return text;
    }
    */

    @Path("{id}/prediction/waittime.txt")
    @GET 
    @Produces("text/plain")
    public String getWaitTimeText(@PathParam("system") String system,
				  @PathParam("id") String jobId,
				  @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionText(true,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/waittime.html")
    @GET 
    @Produces("text/html")
    public String getWaitTimeHtml(@PathParam("system") String system,
				  @PathParam("id") String jobId,
				  @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionHtml(true,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/waittime.xml")
    @GET 
    @Produces("text/xml")
    public String getWaitTimeXml(@PathParam("system") String system,
				 @PathParam("id") String jobId,
				 @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionXml(true,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/waittime.json")
    @GET 
    @Produces("application/json")
    public String getWaitTimeJson(@PathParam("system") String system,
				  @PathParam("id") String jobId,
				  @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionJson(true,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/starttime.txt")
    @GET 
    @Produces("text/plain")
    public String getStartTimeText(@PathParam("system") String system,
				   @PathParam("id") String jobId,
				   @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionText(false,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/starttime.html")
    @GET 
    @Produces("text/html")
    public String getStartTimeHtml(@PathParam("system") String system,
				   @PathParam("id") String jobId,
				   @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionHtml(false,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/starttime.xml")
    @GET 
    @Produces("text/xml")
    public String getStartTimeXml(@PathParam("system") String system,
				  @PathParam("id") String jobId,
				  @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionXml(false,system,jobId,confidenceStr);
    }

    @Path("{id}/prediction/starttime.json")
    @GET 
    @Produces("application/json")
    public String getStartTimeJson(@PathParam("system") String system,
				   @PathParam("id") String jobId,
				   @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getQueuePredictionJson(false,system,jobId,confidenceStr);
    }

    public String getQueuePredictionText(boolean waitPrediction,
					 String system,
					 String jobId,
					 String confidenceStr) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(system)) {
	    return "Unknown system: "+system+"\n";
	}

	int confidence = 0;
	try {
	    confidence = Integer.parseInt(confidenceStr);
	} catch (Exception e) {
	    return "Confidence interval size must be an integer\n";
	}
	if ((confidence < 10) || (confidence > 95)) {
	    return "Confidence interval size must be between 10 and 95\n";
	}

	Job job = GlueDatabase.getCurrentJob(system,jobId);
	if (job == null) {
	    return "Job is unknown on specified system\n";
	}

	Table table = getQueuePredictionTable(waitPrediction,job,confidence);
	return table.getText();
    }

    public String getQueuePredictionHtml(boolean waitPrediction,
					 String system,
					 String jobId,
					 String confidenceStr) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(system)) {
	    return "<p>Unknown system: "+system+"</p>\n";
	}

	int confidence = 0;
	try {
	    confidence = Integer.parseInt(confidenceStr);
	} catch (Exception e) {
	    return "<p>Confidence interval size must be an integer\n";
	}
	if ((confidence < 10) || (confidence > 95)) {
	    return "<p>Confidence interval size must be between 10 and 95\n";
	}

	Job job = GlueDatabase.getCurrentJob(system,jobId);
	if (job == null) {
	    return "<p>Job is unknown on specified system\n";
	}

	Table table = getQueuePredictionTable(waitPrediction,job,confidence);
	return table.getHtml();
    }

    public Table getQueuePredictionTable(boolean waitPrediction,
					 Job job,
					 int confidence) {
	Table table = new Table("Job "+job.id+" on "+job.system);

	if (waitPrediction) {
	    table.setHeader("Submit Time\n("+WebService.getLocalTimeZoneString()+")",
			    "Processors",
			    "Requested Wall Time\n(hours:minutes:seconds)",
			    "Predicted Wait Time\n(hours:minutes:seconds)",
			    confidence+"% Confidence\n(hours:minutes:seconds)");
	} else {
	    table.setHeader("Submit Time\n("+WebService.getLocalTimeZoneString()+")",
			    "Processors",
			    "Requested Wall Time\n(hours:minutes:seconds)",
			    "Predicted Start Time\n("+WebService.getLocalTimeZoneString()+")",
			    confidence+"% Confidence\n(hours:minutes:seconds)");
	}

	String submitTime = "";
	if (job.submitTime != null) {
	    submitTime = WebService.dateToLocalString(job.submitTime);
	} else {
	    // this seems to be happening for some reason
	    submitTime = "unknown";
	}

	if (job.startTime == null) {
	    try {
		Date curTime = new Date();
		SubmittedStartTimeQuery query = new SubmittedStartTimeQuery();
		query.system = job.system;
		query.jobId = job.id;
		query.intervalPercent = confidence;
		SubmittedStartTimePrediction pred = SubmittedPredictor.getPrediction(query, WeightingEnum.Single);
		if (waitPrediction) {
		    int waitTime = (int)((pred.startTime.getTime() - curTime.getTime()) / 1000);
		    if (waitTime < 0) {
			waitTime = 0;
		    }
		    table.addRow(submitTime,
				 String.valueOf(job.processors),
				 WebService.hms(job.requestedWallTime),
				 WebService.hms(waitTime),
				 "\u00B1"+WebService.hms((int)pred.intervalSecs));
		} else {
		    table.addRow(submitTime,
				 String.valueOf(job.processors),
				 WebService.hms(job.requestedWallTime),
				 WebService.dateToLocalString(pred.startTime),
				 "\u00B1"+WebService.hms((int)pred.intervalSecs));
		}
	    } catch (KarnakException e) {
		table.addRow(submitTime,String.valueOf(job.processors),WebService.hms(job.requestedWallTime),
			     "unknown","\u00B1"+"unknown");
	    }
	} else {
	    table.addRow(submitTime,String.valueOf(job.processors),WebService.hms(job.requestedWallTime),
			 "0","\u00B1"+"0");
	}

	return table;
    }

    public String getQueuePredictionXml(boolean waitPrediction,
					String system,
					String jobId,
					String confidenceStr) {
	SubmittedStartTimeQuery query = new SubmittedStartTimeQuery();

	query.system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(query.system)) {
	    return "<Error xmlns='"+SystemsResource.namespace+"'>Unknown system</Error>\n";
	}

	query.jobId = jobId;

	try {
	    query.intervalPercent = Integer.parseInt(confidenceStr);
	} catch (Exception e) {
	    return "<Error xmlns='"+SystemsResource.namespace+
		"'>Confidence interval size must be an integer</Error>\n";
	}
	if ((query.intervalPercent < 10) || (query.intervalPercent > 95)) {
	    return "<Error xmlns='"+SystemsResource.namespace+
		"'>Confidence interval size must be between 10 and 95</Error>\n";
	}

	Job job = GlueDatabase.getCurrentJob(query.system,query.jobId);
	if (job == null) {
	    return "<Error xmlns='"+SystemsResource.namespace+"'>Job is unknown on specified system</Error>\n";
	}

	SubmittedStartTimePrediction pred = null;
	if (job.startTime == null) {
	    try {
		pred = SubmittedPredictor.getPrediction(query, WeightingEnum.Single);
	    } catch (KarnakException e) {
		logger.warn("Failed to predict job "+query.jobId+" on system "+query.system+": "+e.getMessage());
		return "<Error xmlns='"+SystemsResource.namespace+"'>Failed to predict job "+query.jobId+
		    " on system "+query.system+"</Error>\n";
	    }
	}

	TreeNode root = getQueuePredictionTree(waitPrediction,job,pred);
	return root.getXml();
    }

    public String getQueuePredictionJson(boolean waitPrediction,
					 String system,
					 String jobId,
					 String confidenceStr) {

	SubmittedStartTimeQuery query = new SubmittedStartTimeQuery();

	query.system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();
	if (!systems.contains(query.system)) {
	    return "{ \"Error\" : \"Unknown system\" }\n";
	}

	query.jobId = jobId;

	try {
	    query.intervalPercent = Integer.parseInt(confidenceStr);
	} catch (Exception e) {
	    return "{ \"Error\" : \"Confidence interval size must be an integer\" }\n";
	}
	if ((query.intervalPercent < 10) || (query.intervalPercent > 95)) {
	    return "{ \"Error\" : \"Confidence interval size must be between 10 and 95\" }\n";
	}

	Job job = GlueDatabase.getCurrentJob(query.system,query.jobId);
	if (job == null) {
	    return "{ \"Error\" : \"Job is unknown on specified system\" }\n";
	}

	SubmittedStartTimePrediction pred = null;
	if (job.startTime == null) {
	    try {
		pred = SubmittedPredictor.getPrediction(query, WeightingEnum.Single);
	    } catch (KarnakException e) {
		logger.warn("Failed to predict job "+query.jobId+" on system "+query.system+": "+e.getMessage());
		return "{ \"Error\" : \"Failed to predict\" }\n";
	    }
	}

	TreeNode root = getQueuePredictionTree(waitPrediction,job,pred);
	return root.getXml();
    }

    protected TreeNode getQueuePredictionTree(boolean waitPrediction,
					      Job job,
					      SubmittedStartTimePrediction pred) {
	TreeNode root = new TreeNode("QueuePrediction",null,SystemsResource.namespace);
	Date curTime = new Date();
	root.addAttribute(new TreeNode("time",WebService.dateToString(curTime)));
	root.addChild(new TreeNode("System",job.system));
	root.addChild(new TreeNode("Queue",job.queue));
	root.addChild(new TreeNode("Identifier",job.id));
	if (job.submitTime != null) {
	    root.addChild(new TreeNode("SubmitTime",WebService.dateToString(job.submitTime)));
	} else {
	    // this seems to be happening for some reason
	    logger.warn("submit time for job not set:");
	    logger.warn(job);
	}
	root.addChild(new TreeNode("Processors",job.processors));
	root.addChild(new TreeNode("RequestedWallTime",job.requestedWallTime/60.0));
	if (job.startTime == null) {
	    if (waitPrediction) {
		int waitTime = (int)((pred.startTime.getTime() - curTime.getTime()) / 1000);
		if (waitTime < 0) {
		    waitTime = 0;
		}
		TreeNode wtNode = new TreeNode("WaitTime",waitTime/60.0);
		wtNode.addAttribute(new TreeNode("units","minutes"));
		root.addChild(wtNode);
	    } else {
		root.addChild(new TreeNode("StartTime",WebService.dateToString(pred.startTime)));
	    }
	    TreeNode confNode = new TreeNode("ConfidenceInterval",pred.intervalSecs/60);
	    confNode.addAttribute(new TreeNode("units","minutes"));
	    confNode.addAttribute(new TreeNode("confidence",pred.intervalPercent));
	    root.addChild(confNode);
	} else {
	    // start time is set, job is running
	    if (waitPrediction) {
		TreeNode wtNode = new TreeNode("WaitTime",0);
		wtNode.addAttribute(new TreeNode("units","minutes"));
		root.addChild(wtNode);
	    } else {
		root.addChild(new TreeNode("StartTime",WebService.dateToString(job.startTime)));
	    }
	    // no confidence interval needed
	}

	return root;
    }

}
