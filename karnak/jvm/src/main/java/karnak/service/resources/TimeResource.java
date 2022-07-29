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

import java.io.*;
import java.text.*;
import java.util.*;
import java.util.regex.*;
import javax.xml.parsers.*;
import javax.ws.rs.WebApplicationException;
import org.xml.sax.SAXException;
import org.w3c.dom.*;

import org.apache.log4j.Logger;

import karnak.KarnakException;
import karnak.service.predict.UnsubmittedPredictor;
import karnak.service.*;
import karnak.service.predict.*;

public class TimeResource {
    
    private static Logger logger = Logger.getLogger(TimeResource.class.getName());

    public String getSystemsText() {
	List<String> systems = GlueDatabase.getSystemNames();
	String text = "The following systems are known:\n";
	for(String system : systems) {
	    text = text + "    " +system + "\n";
	}
        return text;
    }
    
    public String getSystemsHtml() {
	List<String> systems = GlueDatabase.getSystemNames();
	String text = "";
	text = text + "<title>Systems</title>\n";
	text = text + "<h1 align='center'>Systems</h1>\n";

	text = text + "<p>Use this <a href='predict_form.html'>form</a> to describe a new job for prediction.\n";

	text = text + "<p>Select a system below to get predictions for current jobs.\n";
	text = text + "<ul>";
	for(String system : systems) {
	    text = text + "<li> <a href='system/"+system+"/job/waiting.html'>"+system+"\n";
	}
	text = text + "</ul>";
        return text;
    }
    
    public String getSystemsXml() {
	List<String> systems = GlueDatabase.getSystemNames();

	//URI uri =  uriInfo.getAbsolutePath();

	String text = "<Systems xmlns='"+SystemsResource.namespace+"'>\n";
	for(String system : systems) {
	    text = text + "  <System>\n";
	    text = text + "    <Name>"+system+"</Name>\n";
	    //text = text + "    <URL>"+system+"/job/waiting.xml</URL>\n";
	    text = text + "  </System>\n";
	}
	text = text + "</Systems>";
        return text;
    }
    
    public String getSystemsJson() {
	List<String> systems = GlueDatabase.getSystemNames();
	String text = "{\n";
	text += "  \"Systems\" : [\n";
	for(int i=0;i<systems.size();i++) {
	    text += "    \""+systems.get(i)+"\"";
	    if (i != systems.size()-1) {
		text += ",";
	    }
	    text += "\n";
	}
	text += "  ]\n";
	text += "}\n";
        return text;
    }


    public String getPredictFormHtml(boolean waitPrediction) {
	List<String> systems = GlueDatabase.getSystemNames();
	String text = "";
	if (waitPrediction) {
	    text = text + "<title>Wait Time Predictions</title>\n";
	    text = text + "<h1 align='center'>Wait Time Prediction Form</h1>\n";
	} else {
	    text = text + "<title>Start Time Predictions</title>\n";
	    text = text + "<h1 align='center'>Start Time Prediction Form</h1>\n";
	}
	text = text + "<form action='predict.html'>\n";
	text = text + "<p>Select one or more systems and queues:\n";

	text = text + "<p><table border='1'>\n";
	text = text + "<tr>\n";
	text = text + "<th>System</th>\n";
	text = text + "<th>Queues</th>\n";
	text = text + "</tr>\n";

	for (String system : systems) {
	    text = text + "<tr>\n";

	    text = text + "<td><input type='checkbox' name='system' value='"+system+"' checked='checked'>\n";
	    text = text + ""+system+"</td>\n";

	    List<String> queues = GlueDatabase.getQueueNames(system);

	    text = text + "<td>\n";
	    for (String queue : queues) {
		/* no maxProcessors for SGE or trestles
		if (!GlueDatabase.knownLimits(system,queue)) {
		    continue;
		}
		*/
		if (queue.equals(getDefaultQueue(system))) {
		    text = text + "<input type='checkbox' name='queue' value='"+queue+"@"+system+"'"+
			" checked='checked'>"+queue+"<br>\n";
		} else {
		    text = text + "<input type='checkbox' name='queue' value='"+queue+"@"+system+"'>"+queue+"<br>\n";
		}
	    }

	    text = text + "</td>\n";

	    /* to use a drop down
	    text = text + "<td><select name='"+system+"'>\n";
	    for (String queue : queues) {
		if (queue.equals(getDefaultQueue(system))) {
		    text = text + "<option value='"+queue+"' selected='selected'>"+queue+"</option>\n";
		} else {
		    text = text + "<option value='"+queue+"'>"+queue+"</option>\n";
		}
	    }
	    text = text + "</select></td>\n";
	    */

	    text = text + "</tr>\n";
	}
	text = text + "</table>\n";

	text = text + "<p>Describe your job:\n";
	text = text + "<p>Processing cores: <input type='text' name='cores' size='5' value='1'/><br>\n";
	text = text + "<p>Requested wall time: <input type='text' name='hours' size='3' value='1'/>:"+
	    "<input type='text' name='minutes' size='2' value='0'/>"+
	    "(hours:minutes)<br>\n";

	text = text + "<p>Confidence interval size: <input type='text' name='confidence' value='90' size='2'/>%<br>\n";

	text = text + "<p><input type='submit' value='Submit'>\n";

	text = text + "</form>\n";

        return text;
    }

    public static String getDefaultQueue(String systemName) {
	for (GlueQueue queue : GlueDatabase.getSystem(systemName).queues.values()) {
	    if (queue.isDefault) {
		return queue.name;
	    }
	}
	return null;
    }

    public String getPredictHtml(boolean waitPrediction,
				 List<String> systems, List<String> queueAtSystems,
				 String coresStr,
				 String hours, String minutes,
				 String confidenceStr) {
	String text = "";
	if (waitPrediction) {
	    text = text + "<title>Wait Time Predictions</title>\n";
	    text = text + "<h1 align='center'>Wait Time Predictions</h1>\n";
	} else {
	    text = text + "<title>Start Time Predictions</title>\n";
	    text = text + "<h1 align='center'>Start Time Predictions</h1>\n";
	}

	UnsubmittedStartTimeQuery query = new UnsubmittedStartTimeQuery();

	try {
	    query.processors = Integer.parseInt(coresStr);
	} catch (Exception e) {
	    text = text + "<p>Cores must be an integer\n";
	    return text;
	}
	if (query.processors < 1) {
	    text = text + "<p>Cores must be at least one\n";
	    return text;
	}

	try {
	    query.intervalPercent = Integer.parseInt(confidenceStr);
	} catch (Exception e) {
	    text = text + "<p>Confidence interval size must be an integer\n";
	    return text;
	}
	if ((query.intervalPercent < 10) || (query.intervalPercent > 95)) {
	    text = text + "<p>Confidence interval size must be between 10 and 95\n";
	    return text;
	}

	try {
	    query.requestedWallTime = 60 * (Integer.parseInt(minutes) + 60 * Integer.parseInt(hours));
	} catch (Exception e) {
	    text = text + "<p>Minutes and hours in requested run time must be integers\n";
	    return text;
	}

	text = text + "<p>Prediction for a job that will use "+query.processors+" processing cores for "+
	    hours+" hours and "+minutes+" minutes.\n\n";

	text = text + "<p><table border='1' align='center'>\n";
	text = text + "<tr>\n";
	text = text + "<th>System</th>\n";
	text = text + "<th>Queue</th>\n";
	if (waitPrediction) {
	    text = text + "<th>Predicted Wait Time <br> (hours:minutes:seconds)</th>\n";
	} else {
	    text = text + "<th>Predicted Start Time <br> "+WebService.getLocalTimeZoneString()+"</th>\n";
	}
	text = text + "<th>"+query.intervalPercent+"% confidence <br> (hours:minutes:seconds)</th>\n";
	text = text + "</tr>\n";

	TreeSet<String> selectedSystems = new TreeSet<String>();
	for(String system : systems) {
	    selectedSystems.add(system);
	}
	Date curTime = new Date();
	for(String queueSystem : queueAtSystems) {
	    StringTokenizer tok = new StringTokenizer(queueSystem,"@");
	    query.queue = tok.nextToken();
	    query.system = tok.nextToken();
	    if (selectedSystems.contains(query.system)) {
		text = text + "<tr>\n";
		text = text + "<td align='right'>"+query.system+"</td>\n";
		text = text + "<td align='center'>"+query.queue+"</td>\n";
		if (query.processors > getMaxProcessors(query.system,query.queue)) {
		    if (waitPrediction) {
			text = text + "<td align='right'>N/A</td>\n";
		    } else {
			text = text + "<td align='center'>N/A</td>\n";
		    }
		    text = text + "<td align='right'>N/A</td>\n";
		    text = text + "</tr>\n";
		    continue;
		}
		if (query.requestedWallTime > getMaxWallTime(query.system,query.queue)) {
		    if (waitPrediction) {
			text = text + "<td align='right'>N/A</td>\n";
		    } else {
			text = text + "<td align='center'>N/A</td>\n";
		    }
		    text = text + "<td align='right'>N/A</td>\n";
		    text = text + "</tr>\n";
		    continue;
		}
		try {
		    UnsubmittedStartTimePrediction pred = UnsubmittedPredictor.getPrediction(query);
		    if (waitPrediction) {
			int waitTime = (int)((pred.startTime.getTime() - curTime.getTime()) / 1000);
			if (waitTime < 0) {
			    waitTime = 0;
			}
			text = text + "<td align='right'>"+WebService.hms(waitTime)+"</td>\n";
		    } else {
			text = text + "<td align='center'>"+WebService.dateToLocalString(pred.startTime)+"</td>\n";
		    }
		    text = text + "<td align = 'right'>&plusmn;"+WebService.hms((int)pred.intervalSecs)+"</td>\n";
		} catch (KarnakException e) {
		    if (waitPrediction) {
			text = text + "<td align='right'>unknown</td>\n";
		    } else {
			text = text + "<td align='center'>unknown</td>\n";
		    }
		    text = text + "<td align = 'right'>&plusmn;unknown</td>\n";
		}
		text = text + "</tr>\n";
	    }
	}
	text = text + "</table>\n";

	return text;
    }

    class Location implements Comparable<Location> {
	public String system = "";
	public String queue = "";
	public UnsubmittedStartTimePrediction pred = null;
	public String errorMessage = null;

	public Location(String system, String queue) {
	    this.system = system;
	    this.queue = queue;
	}

	public int compareTo(Location loc) {
	    if (system.compareTo(loc.system) != 0) {
		return system.compareTo(loc.system);
	    }
	    if (queue.compareTo(loc.queue) != 0) {
		return queue.compareTo(loc.queue);
	    }
	    return 0;
	}
    }

    class PredictionRequest {
	public int processors = 1;
	public int wallTimeSecs = 15 * 60;
	public int confidence = 90;
	public List<Location> locations = new ArrayList<Location>();
	public Date submitTime = null;
	public String id = null;

	public PredictionRequest() {
	    synchronized(predictionResources) {
		submitTime = new Date();
		id = String.valueOf(new GregorianCalendar(TimeZone.getTimeZone("GMT")).getTimeInMillis());
		try {
		    Thread.sleep(1);
		} catch (InterruptedException e) {}
	    }
	}

	public String toString() {
	    String str = "PredictionRequest at "+submitTime+"\n";
	    str += "    "+processors+" processors\n";
	    str += "    "+WebService.hms(wallTimeSecs)+" requested wall time\n";
	    str += "    "+confidence+"% confidence\n";
	    for(Location location : locations) {
		str += "        "+location.queue+" on "+location.system+"\n";
	    }
	    return str;
	}
    }

    protected static TreeMap<String,PredictionRequest> predictionResources = new TreeMap<String,PredictionRequest>();

    protected void prunePredictionResources() {
	Date curTime = new Date();
	Iterator<String> keys = predictionResources.keySet().iterator();
	while(keys.hasNext()) {
	    String id = keys.next();
	    if (Long.parseLong(id) < curTime.getTime() - 5 * 60 * 1000) {
		keys.remove();
	    }
	}
	/*
	Set<String> keys = predictionResources.keySet();
	for(String id : keys) {
	    if (Long.parseLong(id) < curTime - 15 * 60 * 1000) {
		keys.remove(id);
	    }
	}
	*/
    }


    public String predictXml(String content) {
	logger.info(content);

	/*
	  <Predictions>
	    <Processors>1</Processors>
            <RequestedWallTime units='minutes'>15</RequestedWallTime>
	    <Confidence>90</Confidence>
            <Location system='system' queue='queue'/>
            <Location system='system' queue='queue'/>
	  </Predictions>
	*/

	DocumentBuilder builder = null;
	try {
	    builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	} catch (ParserConfigurationException e) {
	    logger.error("failed to create factory: "+e.getMessage());
	    throw new WebApplicationException(500);
	}
	Document doc = null;
	try {
	    doc = builder.parse(new ByteArrayInputStream(content.getBytes()));
	} catch (IOException e) {
	    logger.error("failed to read request: "+e.getMessage());
	    throw new WebApplicationException(400);
	} catch (SAXException e) {
	    logger.error("failed to parse request: "+e.getMessage());
	    throw new WebApplicationException(400);
	}

	PredictionRequest req = new PredictionRequest();

	NodeList list = doc.getElementsByTagName("Processors");
	if (list.getLength() == 0) {
	    logger.warn("didn't find Processors");
	    throw new WebApplicationException(400);
	}
	String text = ((Text)(list.item(0).getFirstChild())).getWholeText();
	try {
	    req.processors = Integer.parseInt(text);
	} catch (Exception e) {
	    throw new WebApplicationException(400);
	}

	list = doc.getElementsByTagName("RequestedWallTime");
	if (list.getLength() == 0) {
	    logger.warn("didn't find RequestedWallTime");
	    throw new WebApplicationException(400);
	}
	text = ((Text)(list.item(0).getFirstChild())).getWholeText();
	try {
	    req.wallTimeSecs = Integer.parseInt(text) * 60; // assuming XML has minutes
	} catch (Exception e) {
	    throw new WebApplicationException(400);
	}

	int confidence = 90;
	list = doc.getElementsByTagName("Confidence");
	if (list.getLength() == 0) {
	    logger.warn("using default Confidence");
	} else {
	    text = ((Text)(list.item(0).getFirstChild())).getWholeText();
	    try {
		req.confidence = Integer.parseInt(text);
	    } catch (Exception e) {
		throw new WebApplicationException(400);
	    }
	}

	list = doc.getElementsByTagName("Location");
	if (list.getLength() == 0) {
	    logger.warn("didn't find Location");
	    throw new WebApplicationException(400);
	}
	for(int i=0;i<list.getLength();i++) {
	    String system = ((Attr)(list.item(i).getAttributes().getNamedItem("system"))).getValue();
	    if (system == null) {
		logger.warn("didn't find system attribute of Location");
		throw new WebApplicationException(400);
	    }
	    String queue = ((Attr)(list.item(i).getAttributes().getNamedItem("queue"))).getValue();
	    if (queue == null) {
		logger.warn("didn't find queue attribute of Location");
		throw new WebApplicationException(400);
	    }
	    req.locations.add(new Location(system,queue));
	}

	return predict(req);
    }

    public String predictJson(String content) {
	logger.info(content);
	/*
	  {
	    "Predictions" : [
	      "Processors" : 1,
	      "RequestedWallTime" : [
	        "units" : "minutes",
		"RequestedWallTime" : 15
	      ],
	      "Confidence" : 90,
	      "Location" : [
	        "system" : "system",
		"queue" : "queue"
	      ]
	      "Location" : [
	        "system" : "system",
		"queue" : "queue"
	      ]
	    ]
	  }
	*/

	PredictionRequest req = new PredictionRequest();

	Pattern pattern = Pattern.compile("\"Processors\"\\s*:\\s*(\\d+)");
	Matcher match = pattern.matcher(content);
	try {
	    req.processors = Integer.parseInt(match.group(1));
	} catch (IndexOutOfBoundsException e) {
	    throw new WebApplicationException(400);
	} catch (NumberFormatException e) {
	    throw new WebApplicationException(400);
	}

	pattern = Pattern.compile("\"units\"\\s*:\\s*\"(\\w+)\"");
	match = pattern.matcher(content);
	String units = "minutes";
	try {
	    units = match.group(1);
	} catch (IndexOutOfBoundsException e) {}

	pattern = Pattern.compile("\"RequestedWallTime\"\\s*:\\s*(\\d+.*\\d*)");
	match = pattern.matcher(content);
	try {
	    float wallTime = Float.parseFloat(match.group(1));
	    if (units.equals("seconds")) {
		req.wallTimeSecs = (int)wallTime;
	    } else if (units.equals("minutes")) {
		req.wallTimeSecs = (int)(wallTime * 60.0);
	    } else if (units.equals("hours")) {
		req.wallTimeSecs = (int)(wallTime * 60.0 * 60.0);
	    } else {
		throw new WebApplicationException(400);
	    }
	} catch (IndexOutOfBoundsException e) {
	    throw new WebApplicationException(400);
	} catch (NumberFormatException e) {
	    throw new WebApplicationException(400);
	}

	pattern = Pattern.compile("\"Confidence\"\\s*:\\s*(\\d+)");
	match = pattern.matcher(content);
	req.confidence = 90;
	try {
	    req.confidence = Integer.parseInt(match.group(1));
	} catch (IndexOutOfBoundsException e) {
	} catch (NumberFormatException e) {
	    throw new WebApplicationException(400);
	}

	pattern = Pattern.compile("\"system\"\\s*:\\s*\"(\\w+)\"");
	match = pattern.matcher(content);
	List<String> systems = new Vector<String>();
	for(int i=1;i<match.groupCount()+1;i++) {
	    systems.add(match.group(i));
	}
	pattern = Pattern.compile("\"queue\"\\s*:\\s*\"(\\w+)\"");
	match = pattern.matcher(content);
	List<String> queues = new Vector<String>();
	for(int i=1;i<match.groupCount()+1;i++) {
	    queues.add(match.group(i));
	}
	if (systems.size() != queues.size()) {
	    throw new WebApplicationException(400);
	}

	for(int i=0;i<systems.size();i++) {
	    req.locations.add(new Location(systems.get(i),queues.get(i)));
	}

	return predict(req);
    }

    public String predictText(String content) {
	logger.info(content);

	/*
	  processors 1
	  requestedWallTime hh:mm:ss
	  confidence 90
	  location system queue
	  location system queue
	*/

	PredictionRequest req = new PredictionRequest();

	String[] lines = content.split("\n");
	for(String line : lines) {
	    String[] tok = line.split("\\s");
	    if (tok.length == 0) {
		continue;
	    }
	    if (tok[0].equals("processors")) {
		if (tok.length != 2) {
		    throw new WebApplicationException(400);
		}
		try {
		    req.processors = Integer.parseInt(tok[1]);
		} catch (NumberFormatException e) {
		    logger.error("processors isn't an integer: "+tok[1]);
		    throw new WebApplicationException(400);
		}
	    } else if (tok[0].equals("requestedWallTime")) {
		if (tok.length != 2) {
		    logger.error("wrong number of parameters for requested wall time: "+tok.length);
		    throw new WebApplicationException(400);
		}
		String[] tok2 = tok[1].split(":");
		if (tok2.length != 3) {
		    logger.error("wrong number of tokens for requested wall time: "+tok[1]);
		    throw new WebApplicationException(400);
		}
		try {
		    req.wallTimeSecs = Integer.parseInt(tok2[2]) + 
			60 * (Integer.parseInt(tok2[1]) + 60 * Integer.parseInt(tok2[0]));
		} catch (NumberFormatException e) {
		    logger.error("requested wall time tokens aren't integers: "+tok[1]);
		    throw new WebApplicationException(400);
		}
	    } else if (tok[0].equals("confidence")) {
		if (tok.length != 2) {
		    logger.error("wrong number of parameters for confidence: "+tok.length);
		    throw new WebApplicationException(400);
		}
		try {
		    req.confidence = Integer.parseInt(tok[1]);
		} catch (NumberFormatException e) {
		    logger.error("confidence isn't an integer: "+tok[1]);
		    throw new WebApplicationException(400);
		}
	    } else if (tok[0].equals("location")) {
		if (tok.length != 3) {
		    logger.error("wrong number of parameters for location: "+tok.length);
		    throw new WebApplicationException(400);
		}
		req.locations.add(new Location(tok[1],tok[2]));
	    } else {
		logger.error("unexpected parameter: "+tok[0]);
		throw new WebApplicationException(400);
	    }
	}

	return predict(req);
    }

    protected String predict(PredictionRequest req) {
	logger.debug(req);
	for(Location location : req.locations) {
	    int maxProcs = getMaxProcessors(location.system,location.queue);
	    if (req.processors > maxProcs) {
		location.pred = null;
		location.errorMessage = "requested processors must be less than " + String.valueOf(maxProcs);
		continue;
	    }
	    int maxWallTime = getMaxWallTime(location.system,location.queue);
	    if (req.wallTimeSecs > maxWallTime) {
		location.pred = null;
		location.errorMessage = "requested wall time must be less than " + WebService.hms(maxProcs);
		continue;
	    }

	    try {
		UnsubmittedStartTimeQuery query = new UnsubmittedStartTimeQuery();
		query.system = location.system;
		query.queue = location.queue;
		query.processors = req.processors;
		query.requestedWallTime = req.wallTimeSecs;
		location.pred = UnsubmittedPredictor.getPrediction(query);
	    } catch (KarnakException e) {
		logger.warn("prediction failed: "+e.getMessage());
		location.pred = null;
		location.errorMessage = e.getMessage();
	    }
	}

	prunePredictionResources();
	logger.info("creating prediction with id '"+req.id+"'");
	for(Location location : req.locations) {
	    logger.info("  "+location.queue+"@"+location.system+": "+location.pred);
	}
	predictionResources.put(req.id,req);
	return req.id;
    }

    public static int getMaxProcessors(String systemName, String queueName) {
	int processors = GlueDatabase.getSystems().get(systemName).processors;
	int maxProcessors = GlueDatabase.getSystems().get(systemName).queues.get(queueName).maxProcessors;
	if ((maxProcessors <= 0) || (maxProcessors > processors)) {
	    return processors;
	}
	return maxProcessors;
    }

    public static int getMaxWallTime(String systemName, String queueName) {
	int maxWallTime = GlueDatabase.getSystems().get(systemName).queues.get(queueName).maxWallTime;
	if (maxWallTime <= 0) {
	    return Integer.MAX_VALUE;
	}
	return maxWallTime;
    }



    public String getPredictionText(boolean waitPrediction, String id) {
	prunePredictionResources();
	if (!predictionResources.containsKey(id)) {
	    logger.warn("didn't find prediction resource '"+id+"'");
	    throw new WebApplicationException(400);
	}
	PredictionRequest req = predictionResources.get(id);

	Table table = null;
	if (waitPrediction) {
	    table = new Table("Wait Time Predictions");
	    table.setHeader("System",
			    "Queue",
			    "Predicted Wait Time\n(hours:minutes:seconds)",
			    req.confidence+"% Confidence\n(hours:minutes:seconds)");
	} else {
	    table = new Table("Start Time Predictions");

	    table.setHeader("System",
			    "Queue",
			    "Predicted Start Time\n("+WebService.getLocalTimeZoneString()+")",
			    req.confidence+"% Confidence\n(hours:minutes:seconds)");
	}

	for (Location location : req.locations) {
	    if (location.pred == null) {
		table.addRow(location.system,location.queue,"N/A","N/A");
		continue;
	    }
	    if (waitPrediction) {
		Date curTime = new Date();
		int waitTime = (int)((location.pred.startTime.getTime() - curTime.getTime()) / 1000);
		int confInt = (int)location.pred.intervalSecs;
		logger.info("  wait time prediction for "+location.queue+"@"+location.system+" is "+
			    waitTime+" secs +- "+confInt);
		table.addRow(location.system, location.queue,
			     WebService.hms(waitTime), WebService.hms(confInt));
	    } else {
		table.addRow(location.system,
			     location.queue,
			     WebService.dateToLocalString(location.pred.startTime),
			     WebService.hms((int)location.pred.intervalSecs));
	    }
	}

	String text = "";
	
	int hours = req.wallTimeSecs / (60 * 60);
	int minutes = (req.wallTimeSecs - hours*60*60)/60;
	text += "\nPrediction for a job submitted at "+
	    WebService.dateToLocalString(req.submitTime)+" "+WebService.getLocalTimeZoneString()+" that will\n"+
	    "    use "+req.processors+" processing cores\n"+
	    "    for "+hours+" hours and "+minutes+" minutes.\n\n";

	text += table.getText();

	return text;
    }

    public String getPredictionXml(boolean waitPrediction, String id) {
	prunePredictionResources();
	if (!predictionResources.containsKey(id)) {
	    logger.warn("didn't find prediction resource '"+id+"'");
	    throw new WebApplicationException(400);
	}
	PredictionRequest req = predictionResources.get(id);
	TreeNode root = getPredictionTree(waitPrediction,req);
	return root.getXml();

	/*
	  <Predictions>
	    <Processors>1</Processors>
            <RequestedWallTime units='minutes'>15</RequestedWallTime>
	    <Confidence>90</Confidence>
	    <SubmitTime>2010-04-21T00:00:00Z</SubmitTime>
            <Location system='system' queue='queue'>
	        <WaitTime units='minutes'>60</WaitTime>
		                * or *
	        <StartTime>2010-04-21T00:00:00Z</StartTime>
		                * or *
	        <Error>error message</Error>
                      * or blank, if there was no error, but no prediction could be made *
		<Confidence units='minutes'>76</Confidence>
	    </Location>
            <Location system='system' queue='queue'/>
	        ...
	    </Location>
	  </Predictions>
	*/

	/*
	String text = "";
	text += "<QueuePredictions xmlns='"+SystemsResource.namespace+"'"+
	    " time='"+WebService.dateToString(req.submitTime)+"'>\n";
	text += "  <Processors>"+req.processors+"</Processors>\n";
	text += "  <RequestedWallTime units='minutes'>"+(req.wallTimeSecs/60.0)+"</RequestedWallTime>\n";
	text += "  <Confidence>"+req.confidence+"</Confidence>\n";
	text += "  <SubmitTime>"+WebService.dateToString(req.submitTime)+"</SubmitTime>\n";
	for (Location location : req.locations) {
	    text += "  <Location system='"+location.system+"' queue='"+location.queue+"'>\n";
	    if (location.pred == null) {
		// any output if the cores or wall time isn't valid?
	    } else {
		if (waitPrediction) {
		    long waitTimeSecs = (location.pred.startTime.getTime() - new Date().getTime()) / 1000;
		    text += "    <WaitTime units='minutes'>"+(waitTimeSecs/60.0)+"</WaitTime>\n";
		} else {
		    text += "    <StartTime>"+WebService.dateToString(location.pred.startTime)+"</StartTime>\n";
		}
		text += "    <Confidence units='minutes'>"+
		    (location.pred.conf.getIntervalSize(req.confidence)/60.0)+"</Confidence>\n";
	    }
	    text += "  </Location>\n";
	}
	text += "</QueuePredictions>\n";

	return text;
	*/
    }

    public String getPredictionJson(boolean waitPrediction, String id) {
	prunePredictionResources();
	if (!predictionResources.containsKey(id)) {
	    logger.warn("didn't find prediction resource '"+id+"'");
	    throw new WebApplicationException(400);
	}
	PredictionRequest req = predictionResources.get(id);
	TreeNode root = getPredictionTree(waitPrediction,req);
	return root.getJson();
    }

    public TreeNode getPredictionTree(boolean waitPrediction, PredictionRequest req) {
	TreeNode root = new TreeNode("QueuePredictions",null,SystemsResource.namespace);
	root.addAttribute(new TreeNode("time",WebService.dateToString(req.submitTime)));
	root.addChild(new TreeNode("Processors",req.processors));
	TreeNode reqWallTimeNode = new TreeNode("RequestedWallTime",req.wallTimeSecs/60.0);
	reqWallTimeNode.addAttribute(new TreeNode("units","minutes"));
	root.addChild(reqWallTimeNode);
	root.addChild(new TreeNode("Confidence",req.confidence));
	root.addChild(new TreeNode("SubmitTime",WebService.dateToString(req.submitTime)));

	for (Location location : req.locations) {
	    TreeNode locationNode = new TreeNode("Location");
	    locationNode.addAttribute(new TreeNode("system",location.system));
	    locationNode.addAttribute(new TreeNode("queue",location.queue));
	    if (location.pred == null) {
		if (location.errorMessage != null) {
		    TreeNode errorNode = new TreeNode("Error",location.errorMessage);
		    locationNode.addChild(errorNode);
		}
	    } else {
		if (waitPrediction) {
		    long waitTimeSecs = (location.pred.startTime.getTime() - new Date().getTime()) / 1000;
		    TreeNode wtNode = new TreeNode("WaitTime",waitTimeSecs/60.0);
		    wtNode.addAttribute(new TreeNode("units","minutes"));
		    locationNode.addChild(wtNode);
		} else {
		    locationNode.addChild(new TreeNode("StartTime",WebService.dateToString(location.pred.startTime)));
		}
		TreeNode confNode =
		    new TreeNode("Confidence",location.pred.intervalSecs/60.0);
		confNode.addAttribute(new TreeNode("units","minutes"));
		locationNode.addChild(confNode);
	    }
	    root.addChild(locationNode);
	}

	return root;
    }

}
