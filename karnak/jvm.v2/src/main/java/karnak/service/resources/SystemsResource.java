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

package karnak.service.resources;

import java.util.*;
import javax.ws.rs.*;

import org.apache.log4j.Logger;

import karnak.service.*;

@Path("/karnak/system")
public class SystemsResource {

    private static Logger logger = Logger.getLogger(SystemsResource.class.getName());

    protected static String all_jobs_queue = "all_jobs";

    public static String namespace = "http://tacc.utexas.edu/karnak/protocol/1.0";

    protected static String textHeading(String text, int size) {
	String str = "";
	int leftSize = (size-2-text.length())/2;
	for(int i=0;i<leftSize;i++) {
	    str += "-";
	}
	str += " " + text + " ";
	int rightSize = (size-2-text.length())/2;
	if ((size-2-text.length()) % 2 == 1) {
	    rightSize++;
	}
	for(int i=0;i<rightSize;i++) {
	    str += "-";
	}
	str += "\n";
	return str;
    }

    @Path("status.txt")
    @GET 
    @Produces("text/plain")
    public String getSystemsText() {
	List<String> systems = GlueDatabase.getSystemNames();

	Table table = new Table("System Status");
	table.setHeader("System Name","Running Jobs","Waiting Jobs","Used Processors");

	for(String system : systems) {
	    JobSummary summary = getJobSummary(system);

	    String numRunning = "unknown";
	    if (summary.numRunning != -1) {
		numRunning = String.valueOf(summary.numRunning);
	    }
	    String numWaiting = "unknown";
	    if (summary.numWaiting != -1) {
		numWaiting = String.valueOf(summary.numWaiting);
	    }
	    String usedProcessors = "unknown";
	    if (summary.usedProcessors != -1) {
		usedProcessors = String.valueOf(summary.usedProcessors);
	    }

	    table.addRow(system,String.valueOf(numRunning),String.valueOf(numWaiting),String.valueOf(usedProcessors));
	}

        return table.getText();
    }
    
    @Path("status.html")
    @GET 
    @Produces("text/html")
    public String getSystemsHtml() {
	List<String> systems = GlueDatabase.getSystemNames();

	String text = "";
	text = text + "<title>Systems</title>\n";
	text = text + "<h1 align='center'>Systems</h1>\n";
	text = text + "<table border='1' align='center'>\n";
	text = text + "<caption>System Status</caption>\n";
	text = text + "<tr>\n";
	text = text + "<th>System Name</th>\n";
	text = text + "<th>Running Jobs</th>\n";
	text = text + "<th>Waiting Jobs</th>\n";
	text = text + "<th>Used Processors</th>\n";
	text = text + "</tr>\n";

	for(String system : systems) {
	    text = text + "<tr>\n";
	    text = text + "<td align='center'><a href='"+system+"/status.html'>"+system+"</a></td>\n";

	    JobSummary summary = getJobSummary(system);

	    if (summary.numRunning != -1) {
		text = text + "<td align='right'>"+String.valueOf(summary.numRunning)+"</td>\n";
	    } else {
		text = text + "<td align='right'>unknown</td>\n";
	    }
	    if (summary.numWaiting != -1) {
		text = text + "<td align='right'>"+String.valueOf(summary.numWaiting)+"</td>\n";
	    } else {
		text = text + "<td align='right'>unknown</td>\n";
	    }
	    if (summary.usedProcessors != -1) {
		text = text + "<td align='right'>"+String.valueOf(summary.usedProcessors)+"</td>\n";
	    } else {
		text = text + "<td align='right'>unknown</td>\n";
	    }

	    text = text + "</tr>\n";
	}

	text = text + "</table>\n";

        return text;
    }
    
    @Path("status.xml")
    @GET 
    @Produces("text/xml")
    public String getSystemsXml() {
	List<String> systems = GlueDatabase.getSystemNames();

	String text = "<Systems xmlns='"+namespace+"'>\n";
	for(String system : systems) {
	    text = text + "  <System>\n";
	    text = text + "    <Name>"+system+"</Name>\n";
	    JobSummary summary = getJobSummary(system);
	    text = text + getJobSummaryXml(summary,"    ");
	    text = text + "  </System>\n";
	}
	text = text + "</Systems>\n";

        return text;
    }

    /*    
    @Path("status.json")
    @GET 
    @Produces("application/json")
    public String getSystemsXml() {
	List<String> systems = GlueDatabase.getSystemNames();

	String text = "{\n";
	text += "  \"Systems\" : [\n";
	for(int i=0;i<systems.size();i++) {
	    text += "    
	    text = text + "  <System>\n";
	    text = text + "    <Name>"+system+"</Name>\n";
	    JobSummary summary = getJobSummary(system);
	    text = text + getJobSummaryXml(summary,"    ");
	    text = text + "  </System>\n";
	}
	text = text + "</Systems>\n";

        return text;
    }
    */

    @Path("{system}/status.txt")
    @GET 
    @Produces("text/plain")
    public String getQueuesText(@PathParam("system") String system) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();

	if (!systems.contains(system)) {
	    return "Unknown system: "+system+"\n";
	}

	List<String> queues = GlueDatabase.getQueueNames(system);

	Table table = new Table("Queue Status for "+system);
	table.setHeader("Queue Name","Running Jobs","Waiting Jobs","Used Processors");

	JobSummary total = new JobSummary(system,all_jobs_queue);
	for(String queue : queues) {
	    JobSummary summary = getJobSummary(system,queue);
	    total.increment(summary);
	    table.addRow(getQueueLine(summary,true));
	}
	table.setFooter(getQueueLine(total,true));

        return table.getText();
    }

    protected String[] getQueueLine(JobSummary summary, boolean includeQueueName) {
	List<String> line = new ArrayList<String>();
	if (includeQueueName) {
	    line.add(summary.queue);
	}

	if (summary.numRunning == -1) {
	    line.add("unknown");
	} else {
	    line.add(String.valueOf(summary.numRunning));
	}
	if (summary.numWaiting == -1) {
	    line.add("unknown");
	} else {
	    line.add(String.valueOf(summary.numWaiting));
	}
	if (summary.usedProcessors == -1) {
	    line.add("unknown");
	} else {
	    line.add(String.valueOf(summary.usedProcessors));
	}
	return line.toArray(new String[0]);
    }

    @Path("{system}/status.html")
    @GET 
    @Produces("text/html")
    public String getQueuesHtml(@PathParam("system") String system) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();

	if (!systems.contains(system)) {
	    return "<p>Unknown system: "+system+"</p>\n";
	}

	List<String> queues = GlueDatabase.getQueueNames(system);

	String text = "";

	text = text + "<title>System "+system+"</title>\n";
	text = text + "<h1 align='center'>System "+system+"</h1>\n";

	text = text + "<table border='1' align='center'>\n";
	text = text + "<caption>Queue Status</caption>\n";
	text = text + "<tr>\n";
	text = text + "<th>Queue Name</th>\n";
	text = text + "<th>Running Jobs</th>\n";
	text = text + "<th>Waiting Jobs</th>\n";
	text = text + "<th>Used Processors</th>\n";
	text = text + "</tr>\n";

	JobSummary total = new JobSummary(system,all_jobs_queue);
	for(String queue : queues) {
	    JobSummary summary = getJobSummary(system,queue);
	    text = text + getQueueLineHtml(summary);
	    total.increment(summary);
	}
	text = text + getQueueLineHtml(total);
	text = text + "</table>\n";

        return text;
    }

    protected String getQueueLineHtml(JobSummary summary) {
	String text = "<tr>\n";
	text = text + "<td align='center'><a href='queue/"+summary.queue+"/summary.html'>"+summary.queue+"</a></td>\n";
	if (summary.numRunning != -1) {
	    text = text + "<td align='right'>"+summary.numRunning+"</td>\n";
	} else {
	    text = text + "<td align='right'>unknown</td>\n";
	}
	if (summary.numWaiting != -1) {
	    text = text + "<td align='right'>"+summary.numWaiting+"</td>\n";
	} else {
	    text = text + "<td align='right'>unknown</td>\n";
	}
	if (summary.usedProcessors != -1) {
	    text = text + "<td align='right'>"+summary.usedProcessors+"</td>\n";
	} else {
	    text = text + "<td align='right'>unknown</td>\n";
	}
	text = text + "</tr>\n";
	return text;
    }

    @Path("{system}/status.xml")
    @GET 
    @Produces("text/xml")
    public String getQueuesXml(@PathParam("system") String system) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();

	if (!systems.contains(system)) {
	    return "<Error xmlns='"+namespace+"'>unknown system "+system+"</Error>\n";
	}

	String text = "<System xmlns='"+namespace+"'>\n";
	text = text + "  <Name>"+system+"</Name>\n";

	List<String> queues = GlueDatabase.getQueueNames(system);

	JobSummary total = new JobSummary(system,all_jobs_queue);
	for(String queue : queues) {
	    JobSummary summary = getJobSummary(system,queue);
	    text = text + getQueueLineXml(summary);
	    total.increment(summary);
	}
	text = text + getQueueLineXml(total);
	text = text + "</System>\n";
	return text;
    }

    protected String getQueueLineXml(JobSummary summary) {
	String text = "  <Queue>\n";
	text = text + "    <Name>"+summary.queue+"</Name>\n";
	text = text + getJobSummaryXml(summary,"    ");
	text = text + "  </Queue>\n";
	return text;
    }

    protected String getJobSummaryXml(JobSummary summary, String indent) {
	String text = null;
	if (summary.time != null) {
	    text = indent + "<JobSummary time='"+WebService.dateToString(summary.time)+"'>\n";
	} else {
	    text = indent + "<JobSummary>\n";
	}
	if (summary.numRunning != -1) {
	    text = text + indent + "  <NumRunningJobs>"+summary.numRunning+"</NumRunningJobs>\n";
	}
	if (summary.numWaiting != -1) {
	    text = text + indent + "  <NumWaitingJobs>"+summary.numWaiting+"</NumWaitingJobs>\n";
	}
	if (summary.usedProcessors != -1) {
	    text = text + indent + "  <UsedProcessors>"+summary.usedProcessors+"</UsedProcessors>\n";
	}
	text = text + indent+ "</JobSummary>\n";
	return text;
    }

    @Path("{system}/queue/{queue}/summary.txt")
    @GET 
    @Produces("text/plain")
    public String getQueueText(@PathParam("system") String system, @PathParam("queue") String queue) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();

	if (!systems.contains(system)) {
	    return "Unknown system: "+system+"\n";
	}

	List<String> queues = GlueDatabase.getQueueNames(system);

	if (!queues.contains(queue) && !queue.equals(all_jobs_queue)) {
	    return "Unknown queue "+queue+" for system "+system+"\n";
	}

	Table statusTable = new Table("Status");
	statusTable.setHeader("Running Jobs","Waiting Jobs","Used Processors");

	if (queue.equals(all_jobs_queue)) {
	    queue = null;
	}

	JobSummary summary = getJobSummary(system,queue);
	statusTable.addRow(getQueueLine(summary,false));

	JobStatistics hourStats = JobStatisticsCache.getJobStatistics(system,queue,60*60);
	JobStatistics fourHourStats = JobStatisticsCache.getJobStatistics(system,queue,4*60*60);
	JobStatistics dayStats = JobStatisticsCache.getJobStatistics(system,queue,24*60*60);
	JobStatistics weekStats = JobStatisticsCache.getJobStatistics(system,queue,7*24*60*60);

	Table startedTable = new Table("Started Jobs");
	startedTable.setHeader("When",
			       "Number of\nJobs",
			       "Mean\nProcessors",
			       "Mean Requested Wall Time\n(hours:minutes:seconds)",
			       "Mean Wait Time\n(hours:minutes:seconds)");

	startedTable.addRow(getStartedStatsRow(hourStats,"last hour"));
	startedTable.addRow(getStartedStatsRow(fourHourStats,"last four hours"));
	startedTable.addRow(getStartedStatsRow(dayStats,"last day"));
	startedTable.addRow(getStartedStatsRow(weekStats,"last week"));

	Table completedTable = new Table("Completed Jobs");
	completedTable.setHeader("When",
				 "Number of\nJobs",
				 "Mean\nProcessors",
				 "Mean Requested Wall Time\n(hours:minutes:seconds)",
				 "Mean Wall Time\n(hours:minutes:seconds)");

	completedTable.addRow(getCompletedStatsRow(hourStats,"last hour"));
	completedTable.addRow(getCompletedStatsRow(fourHourStats,"last four hours"));
	completedTable.addRow(getCompletedStatsRow(dayStats,"last day"));
	completedTable.addRow(getCompletedStatsRow(weekStats,"last week"));

	statusTable.setIndent((startedTable.getTotalWidth()-statusTable.getTotalWidth())/2);

	if (queue == null) {
	    queue = "all jobs";
	}
	return textHeading(queue+" on "+system,startedTable.getTotalWidth())+"\n"+
	    statusTable.getText()+"\n"+
	    startedTable.getText()+"\n"+
	    completedTable.getText();
    }

    private String[] getStartedStatsRow(JobStatistics stats, String when) {
	String[] row = new String[5];
	row[0] = when;
	row[1] = String.valueOf(stats.numStartedJobs);
	row[2] = String.valueOf(stats.getMeanProcessors());
	row[3] = stats.getMeanRequestedRunTimeHMS();
	row[4] = stats.getMeanWaitTimeHMS();
	return row;
    }

    private String[] getCompletedStatsRow(JobStatistics stats, String when) {
	String[] row = new String[5];
	row[0] = when;
	row[1] = String.valueOf(stats.numCompletedJobs);
	row[2] = String.valueOf(stats.getMeanCompletedProcessors());
	row[3] = stats.getMeanCompletedRequestedRunTimeHMS();
	row[4] = stats.getMeanRunTimeHMS();
	return row;
    }


    @Path("{system}/queue/{queue}/summary.html")
    @GET 
    @Produces("text/html")
    public String getQueueHtml(@PathParam("system") String system, @PathParam("queue") String queue) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();

	if (!systems.contains(system)) {
	    return "<p>Unknown system: "+system+"</p>\n";
	}

	List<String> queues = GlueDatabase.getQueueNames(system);

	if (!queues.contains(queue) && !queue.equals(all_jobs_queue)) {
	    return "<p>Unknown queue "+queue+" for system "+system+"</p>\n";
	}

	String text = "";

	text = text + "<title>"+queue+" on "+system+"</title>\n";
	text = text + "<h1 align='center'>System "+system+"<br>Queue "+queue+"</h1>\n";

	text = text + "<table border='1' align='center'>\n";
	text = text + "<caption>Status</caption>\n";
	text = text + "<tr>\n";
	text = text + "<th>Running Jobs</th>\n";
	text = text + "<th>Waiting Jobs</th>\n";
	text = text + "<th>Used Processors</th>\n";
	text = text + "</tr>\n";

	if (queue.equals(all_jobs_queue)) {
	    queue = null;
	}

	JobSummary summary = getJobSummary(system,queue);
	text = text + "<tr>\n";
	if (summary.numRunning != -1) {
	    text = text + "<td align='right'>"+summary.numRunning+"</td>\n";
	} else {
	    text = text + "<td align='right'>unknown</td>\n";
	}
	if (summary.numWaiting != -1) {
	    text = text + "<td align='right'>"+summary.numWaiting+"</td>\n";
	} else {
	    text = text + "<td align='right'>unknown</td>\n";
	}
	if (summary.usedProcessors != -1) {
	    text = text + "<td align='right'>"+summary.usedProcessors+"</td>\n";
	} else {
	    text = text + "<td align='right'>unknown</td>\n";
	}
	text = text + "</tr>\n";
	text = text + "</table>\n";

	text = text + "<br><br>\n";

	JobStatistics hourStats = JobStatisticsCache.getJobStatistics(system,queue,60*60);
	JobStatistics fourHourStats = JobStatisticsCache.getJobStatistics(system,queue,4*60*60);
	JobStatistics dayStats = JobStatisticsCache.getJobStatistics(system,queue,24*60*60);
	JobStatistics weekStats = JobStatisticsCache.getJobStatistics(system,queue,7*24*60*60);

	text = text + "<table border='1' align='center'>\n";
	text = text + "<caption>Started Jobs</caption>\n";
	text = text + "<tr>\n";
	text = text + "<th>When</th>\n";
	text = text + "<th>Number of Jobs</th>\n";
	text = text + "<th>Mean Processors</th>\n";
	text = text + "<th>Mean Requested Wall Time <br> (hours:minutes:seconds)</th>\n";
	text = text + "<th>Mean Wait Time <br> (hours:minutes:seconds)</th>\n";
	text = text + "</tr>\n";
	text = text + getStatsForStartedHtml(hourStats,"last hour");
	text = text + getStatsForStartedHtml(fourHourStats,"last four hours");
	text = text + getStatsForStartedHtml(dayStats,"last day");
	text = text + getStatsForStartedHtml(weekStats,"last week");
	text = text + "</table>\n";

	text = text + "<br><br>\n";

	text = text + "<table border='1' align='center'>\n";
	text = text + "<caption>Completed Jobs</caption>\n";
	text = text + "<tr>\n";
	text = text + "<th>When</th>\n";
	text = text + "<th>Number of Jobs</th>\n";
	text = text + "<th>Mean Processors</th>\n";
	text = text + "<th>Mean Requested Wall Time <br> (hours:minutes:seconds)</th>\n";
	text = text + "<th>Mean Wall Time <br> (hours:minutes:seconds)</th>\n";
	text = text + "</tr>\n";
	text = text + getStatsForCompletedHtml(hourStats,"last hour");
	text = text + getStatsForCompletedHtml(fourHourStats,"last four hours");
	text = text + getStatsForCompletedHtml(dayStats,"last day");
	text = text + getStatsForCompletedHtml(weekStats,"last week");
	text = text + "</table>\n";

        return text;
    }

    private String getStatsForStartedHtml(JobStatistics stats, String when) {
	String text = "<tr>\n";
	text = text + "<td align='center'>"+when+"</td>\n";
	text = text + "<td align='right'>"+stats.numStartedJobs+"</td>\n";
	text = text + "<td align='right'>"+stats.getMeanProcessors()+"</td>\n";
	text = text + "<td align='center'>"+stats.getMeanRequestedRunTimeHMS()+"</td>\n";
	text = text + "<td align='center'>"+stats.getMeanWaitTimeHMS()+"</td>\n";
	text = text + "</tr>\n";
	return text;
    }

    private String getStatsForCompletedHtml(JobStatistics stats, String when) {
	String text = "<tr>\n";
	text = text + "<td align='center'>"+when+"</td>\n";
	text = text + "<td align='right'>"+stats.numCompletedJobs+"</td>\n";
	text = text + "<td align='right'>"+stats.getMeanCompletedProcessors()+"</td>\n";
	text = text + "<td align='center'>"+stats.getMeanCompletedRequestedRunTimeHMS()+"</td>\n";
	text = text + "<td align='center'>"+stats.getMeanRunTimeHMS()+"</td>\n";
	text = text + "</tr>\n";
	return text;
    }


    @Path("{system}/queue/{queue}/summary.xml")
    @GET 
    @Produces("text/xml")
    public String getQueueXml(@PathParam("system") String system, @PathParam("queue") String queue) {
	system = system.replace("teragrid.org","xsede.org");
	List<String> systems = GlueDatabase.getSystemNames();

	String text = "";
	text = text + "<System xmlns='"+namespace+"'>\n";
	text = text + "  <Name>"+system+"</Name>\n";

	if (!systems.contains(system)) {
	    return "<Error xmlns='"+namespace+"'>unknown system "+system+"</Error>\n";
	}

	List<String> queues = GlueDatabase.getQueueNames(system);

	if (!queues.contains(queue) && !queue.equals(all_jobs_queue)) {
	    return "<Error xmlns='"+namespace+"'>unknown queue "+queue+" on system "+system+"</Error>\n";
	}

	if (queue.equals(all_jobs_queue)) {
	    queue = null;
	}
	JobSummary summary = getJobSummary(system,queue);

	text = text + "  <Queue>\n";
	text = text + "    <Name>"+queue+"</Name>\n";
	text = text + getJobSummaryXml(summary,"    ");

	JobStatistics hourStats = JobStatisticsCache.getJobStatistics(system,queue,60*60);
	JobStatistics fourHourStats = JobStatisticsCache.getJobStatistics(system,queue,4*60*60);
	JobStatistics dayStats = JobStatisticsCache.getJobStatistics(system,queue,24*60*60);
	JobStatistics weekStats = JobStatisticsCache.getJobStatistics(system,queue,7*24*60*60);

	text = text + getStatsXml(hourStats);
	text = text + getStatsXml(fourHourStats);
	text = text + getStatsXml(dayStats);
	text = text + getStatsXml(weekStats);

	text = text + "  </Queue>\n";
	text = text + "</System>\n";
        return text;
    }

    private String getStatsXml(JobStatistics stats) {
	Date startTime = new Date(stats.curTime.getTime()-stats.historySecs*1000);
	String text = "    <JobStatistics start='"+WebService.dateToString(startTime)+"' "+
	    "end='"+WebService.dateToString(stats.curTime)+"'>\n";
	text = text + "      <Started>\n";
	text = text + "        <NumJobs>"+stats.numStartedJobs+"</NumJobs>\n";
	text = text + "        <MeanProcessors>"+stats.getMeanProcessors()+"</MeanProcessors>\n";
	text = text + "        <MeanRequestedRunTime>"+stats.getMeanRequestedRunTimeHMS()+
	    "</MeanRequestedRunTime>\n";
	text = text + "        <MeanWaitTime>"+stats.getMeanWaitTimeHMS()+"</MeanWaitTime>\n";
	text = text + "      </Started>\n";
	text = text + "      <Completed>\n";
	text = text + "        <NumJobs>"+stats.numCompletedJobs+"</NumJobs>\n";
	text = text + "        <MeanProcessors>"+stats.getMeanCompletedProcessors()+"</MeanProcessors>\n";
	text = text + "        <MeanRequestedRunTime>"+stats.getMeanCompletedRequestedRunTimeHMS()+
	    "</MeanRequestedRunTime>\n";
	text = text + "        <MeanRunTime>"+stats.getMeanRunTimeHMS()+"</MeanRunTime>\n";
	text = text + "      </Completed>\n";
	text = text + "    </JobStatistics>\n";
	return text;
    }


    private JobSummary getJobSummary(String system) {
	return getJobSummary(system,null);
    }

    private JobSummary getJobSummary(String system, String queue) {
	logger.debug("getJobSummary for "+queue+" on "+system);

	JobSummary summary = new JobSummary(system,queue);

	QueueState queueState = GlueDatabase.readCurrentQueueState(system);
	if (queueState == null) {
	    return summary;
	}

	int numRunning = 0;
	int numWaiting = 0;
	int usedProcessors = 0;
	for(Job job : queueState.jobs.values()) {
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

}
