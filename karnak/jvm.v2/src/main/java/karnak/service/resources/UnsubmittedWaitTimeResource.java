/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * This class accepts users' requests for /v2/karnak/waittime.
 *
 * Until the tests are completed, the /v2 path will be used.
 *
 * It only support html response now. I want to make this class not dependent on
 * the TimeResource class so chose not to inherit from it.
 *
 */
package karnak.service.resources;

import java.util.*;
import java.text.SimpleDateFormat;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import karnak.service.*;
import org.apache.log4j.Logger;

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
import karnak.service.resources.TimeResource;
import karnak.service.response.ResponseFactory;
import karnak.service.response.AbstractResponse;
import karnak.service.response.ResponseManager;
import karnak.service.response.UnsubmittedPredictionResponse;
import karnak.service.representation.Estimation;

/* Users can access both URLs */
@Path("/karnak/v2/waittime")
public class UnsubmittedWaitTimeResource {

private static Logger logger = Logger.getLogger(UnsubmittedWaitTimeResource.class.getName());

private static final String[] availableReprs = {"Group", "LabeledGroup", "Raw"};

private final Estimation esttype = Estimation.WAITTIME;

@Path("index.html")
@GET
@Produces("text/html")
public String getSystemsHtml() {

        List<String> systems = GlueDatabase.getSystemNames();

        StringBuilder htmlBuilder = new StringBuilder();
        BufferedReader in = null;

        try {
                in = new BufferedReader(new FileReader("/home/karnak/karnak/var/www/index.html"));
                String str;
                while ((str = in.readLine()) != null) {

                        htmlBuilder.append(str);
                        htmlBuilder.append('\n');

                        /* "<p>list of systems</p>") is a placeholder and keyword
                           to insert the list of systems that currently supports
                           submitted jobs' prediction time.
                           DO NOT REMOVE "<p>list of systems</p>" directive from
                           /home/karnak/karnak/var/www/index.html
                         */
                        if( str.equalsIgnoreCase("<p>list of systems</p>")) {
                                htmlBuilder.append("<h1>\n");
                                htmlBuilder.append("<ul>\n");
                                for (String system : systems) {
                                        htmlBuilder.append( "<li> <a href='system/");
                                        htmlBuilder.append( system );
                                        htmlBuilder.append( "/job/waiting.html'>");
                                        htmlBuilder.append(  system );
                                        htmlBuilder.append( "</a></li>\n");
                                }
                                htmlBuilder.append( "</ul></h1>\n");
                                //htmlBuilder.append("<br><br>");
                        }
                }
        } catch (IOException ex) {
                ex.printStackTrace();
        }finally{
                try{
                        in.close();
                }catch(IOException iox) {}
        }
        return htmlBuilder.toString();

/*
        logger.info("index.html is called");

        List<String> systems = GlueDatabase.getSystemNames();

        StringBuilder htmlBuilder = new StringBuilder();
        htmlBuilder.append("<title>Karnak Wait Time Estimation service v2 </title>\n");
        htmlBuilder.append("<h1 align='center'>Karnak wait time predictions</h1>\n");
        htmlBuilder.append("<body>\n");



        htmlBuilder.append("<h1> If you want predictions for hypothetical jobs (not yet submitted),  </h1>\n");
        htmlBuilder.append("<p>Use this <a href='predict_form.html'>form</a> to describe a new job for prediction.\n");

        htmlBuilder.append("<h1> If you want to know remaining wait times for current jobs (already submitted and in the queue),  choose system </h1>\n");
        htmlBuilder.append("<ul>");
        for (String system : systems) {
                htmlBuilder.append( "<li> <a href='system/");
                htmlBuilder.append( system );
                htmlBuilder.append( "/job/waiting.html'>");
                htmlBuilder.append(  system );
                htmlBuilder.append( "</a></li>\n");
        }
        htmlBuilder.append( "</ul>");


        htmlBuilder.append("<br><br>");
        htmlBuilder.append("<h2> Getting prediction results in  alternative formats </h2>\n");
        htmlBuilder.append("<h2> Start time  or Wait time </h2>\n ");

        htmlBuilder.append("Clients or users can query the estimated start time or wait time of an unsubmitted job for multiple queue@system’s <br>");
        htmlBuilder.append("<ul><li>	   To predict start time, please use “kwouldstart” </li>\n");
        htmlBuilder.append("<li> To predict wait time, please use “kwouldwait” </li></ul>\n");

        htmlBuilder.append("<h2> Prediction grouping ( Raw, Group, LabeledGroup) </h2> \n");
        htmlBuilder.append("Raw representation shows every detail of an estimate generated for given set of system name and queue name's.<br>");
        htmlBuilder.append("It includes estimated time, confidence interval corresponding to requested confidence level.<br><br>\n");
        htmlBuilder.append("Group representation sorts multiple predictions in increasing order by their estimation times. <br>");
        htmlBuilder.append("Fastest queue@system is assigned to the first group (group 1) and the slowest one to the last group.<br> ");
        htmlBuilder.append("if they choose the group representation as they are assigned automatically to each queue@system.<br><br>\n");
        htmlBuilder.append("LabeledGroup representation is very similar to Group representation except labeling. <br> ");
        htmlBuilder.append("It sorts the multiple queue@system’s by time in increasing order as Group representation does. <br>");
        htmlBuilder.append("But it does not use group numbers as labels. <br>");
        htmlBuilder.append("Instead user supplied labels are used to designate groups. <br>");
        htmlBuilder.append("For example, instead of 1,2,3, users may want to tag groups by Blue,Yellow, Red or Fast,medium,Slow and so on. <br>");
        htmlBuilder.append("Please be careful not to include any empty characters or spaces to the labels as this fails correct parsing.<br>\n");

        htmlBuilder.append("<h2> Response format ( HTML, XML, JSON, TXT) </h2>\n");
        htmlBuilder.append("The prediction response can come with various formats.It can be HTML, XML, JSON or TEXT.<br>\n");
        htmlBuilder.append("Please use appropriate option for  “kwouldstart” or”kwouldwait” as follows: <br>\n");
        htmlBuilder.append("<ul><li>	“-t” for text </li>");
        htmlBuilder.append("<li>	“-x” for xml </li> ");
        htmlBuilder.append("<li>       “-j” for json </li></ul>");
        htmlBuilder.append("HTML format is only supported from the browser. <br> Use this  <a href='predict_form.html'>form</a> to describe a new job for prediction.<br>\n");

        htmlBuilder.append("<p>If you wish to access the Karnak service via the command line, ");
        htmlBuilder.append("<a href='karnak-client-v2.zip'>client programs</a> are available ");
        htmlBuilder.append("(Python interpreter required).\n");

        htmlBuilder.append("<p>If you wish to access the Karnak service using Java, ");
        htmlBuilder.append("<a href='karnak-java-client-v2.zip'> Java client library</a> is available ");
        htmlBuilder.append("(Required Apache http client libraries are included).\n");

        htmlBuilder.append("<p>If you wish to download the test document, ");
        htmlBuilder.append("<a href='karnak-testing.pdf'>Test instruction document</a> is available\n ");

        return htmlBuilder.toString();
 */
}

@Path("predict_form.html")
@GET
@Produces("text/html")
public String getPredictFormHtml() {

        logger.info("predict_form.html is called");

        List<String> systems = GlueDatabase.getSystemNames();

        StringBuilder htmlBuilder = new StringBuilder();

        htmlBuilder.append("<title>Wait Time/Start Time Predictions</title>\n");
        htmlBuilder.append("<h1 align='center'>Wait Time/Start time Prediction Form</h1>\n");
        htmlBuilder.append("<form action='predict.html'>\n");
        htmlBuilder.append("<p>Select one or more systems and queues:\n");
        htmlBuilder.append("<p><table border='1'>\n");
        htmlBuilder.append("<tr>\n");
        htmlBuilder.append("<th>System</th>\n");
        htmlBuilder.append("<th>Queues</th>\n");
        htmlBuilder.append("</tr>\n");

        for (String system : systems) {

                htmlBuilder.append("<tr>\n");
                htmlBuilder.append("<td><input type='checkbox' name='system' value='");
                htmlBuilder.append(system);

                htmlBuilder.append("' checked='checked'>\n");
                htmlBuilder.append(system);
                htmlBuilder.append("</td>\n");

                List< String> queues = GlueDatabase.getQueueNames(system);

                htmlBuilder.append("<td>\n");
                for (String queue : queues) {
                        /* no maxProcessors for SGE or trestles
                           if (!GlueDatabase.knownLimits(system,queue)) {
                           continue;
                           }
                         */
                        if (queue.equals(TimeResource.getDefaultQueue(system))) {
                                htmlBuilder.append("<input type='checkbox' name='queue' value='");
                                htmlBuilder.append(queue);
                                htmlBuilder.append("@");
                                htmlBuilder.append(system);
                                htmlBuilder.append("'");
                                htmlBuilder.append(" checked='checked'>");
                                htmlBuilder.append(queue);
                                htmlBuilder.append("<br>\n");
                        } else {
                                htmlBuilder.append("<input type='checkbox' name='queue' value='");
                                htmlBuilder.append(queue);
                                htmlBuilder.append("@");
                                htmlBuilder.append(system);
                                htmlBuilder.append("'>");
                                htmlBuilder.append(queue);
                                htmlBuilder.append("<br>\n");
                        }
                }

                htmlBuilder.append("</td>\n");
                htmlBuilder.append("</tr>\n");
        }
        htmlBuilder.append("</table>\n");
        htmlBuilder.append("<p>Describe your job: <br> \n");

        htmlBuilder.append("Select the prediction output format: ");
        htmlBuilder.append("<select name='PredOutputFormat'>\n");
        for (String repr : availableReprs) {

                htmlBuilder.append("<option value='");
                htmlBuilder.append(repr);
                htmlBuilder.append("'>");
                htmlBuilder.append(repr);
                htmlBuilder.append("</option>\n");
        }
        htmlBuilder.append("</select>\n");
        htmlBuilder.append("<p>Number of groups for Group representation: <input type='text' name='nGroups' size='5' value='3'/> ( relevant only if you choose Group prediction format )<br>\n");
        htmlBuilder.append("<p>Comma seperated labels for LabeledGroup representation : <input type='text' name='labels' size='15' value='Fast,medium,slow'/> ( relevant only if you choose LabeledGroup prediction format )<br>\n");

        htmlBuilder.append("<p>Processing cores: <input type='text' name='cores' size='5' value='1'/><br>\n");
        htmlBuilder.append("<p>Requested wall time: <input type='text' name='hours' size='3' value='1'/>:");
        htmlBuilder.append("<input type='text' name='minutes' size='2' value='0'/>");
        htmlBuilder.append("(hours:minutes)<br>\n");
        htmlBuilder.append("<p>Confidence interval size: <input type='text' name='confidence' value='90' size='2'/>%<br>\n");
        htmlBuilder.append("<p><input type='submit' value='Submit'>\n");
        htmlBuilder.append("</form>\n");

        return htmlBuilder.toString();
}

@Path("predict.html")
@GET
@Produces("text/html")
public String getPredictHtml(@QueryParam("system") List<String> systems,
                             @QueryParam("queue") List<String> queueAtSystems,
                             /* if users do not choose anything, grouped prediction results will be returned */
                             @DefaultValue("1") @QueryParam("cores") String coresStr,
                             @DefaultValue("1") @QueryParam("hours") String hours,
                             @DefaultValue("0") @QueryParam("minutes") String minutes,
                             @DefaultValue("90") @QueryParam("confidence") String confidenceStr,
                             @DefaultValue("Group") @QueryParam("PredOutputFormat") String predictionFormat,
                             @DefaultValue("3") @QueryParam("nGroups") String nGroups,
                             @DefaultValue("Fast,medium,slow") @QueryParam("labels") String strlabels
                             ) {
        logger.debug("predict.html is called");

        for (String system : systems) {
                logger.debug("systems:" + system);
        }

        for (String queue : queueAtSystems) {
                logger.debug("queue:" + queue);
        }

        logger.debug("cores:" + coresStr);
        logger.debug("hours:" + hours);
        logger.debug("minutes:" + minutes);
        logger.debug("confidence:" + confidenceStr);
        logger.debug("PredOutputFormat:" + predictionFormat);

        List<String> labels = Arrays.asList(strlabels.split(","));
        for (String label : labels) {
                logger.debug("label:" + label);
        }

        AbstractResponse resp = ResponseFactory.newResponse(esttype, queueAtSystems, coresStr, hours, minutes, confidenceStr, predictionFormat, Integer.parseInt(nGroups), labels);
        return resp.getHTML();
}

@Path("prediction/")
@POST
@Consumes("text/plain")
@Produces("text/plain")
public String postPredictText(String content,
                              @Context UriInfo uriInfo) {

        AbstractResponse resp = ResponseFactory.newTextResponse(esttype, content);

        ResponseManager manager = ResponseManager.getInstance();
        String id = manager.addResponse((UnsubmittedPredictionResponse) resp);

        logger.info("postPredictText created following xml");
        logger.info(resp.getTEXT());

        UriBuilder ub = uriInfo.getAbsolutePathBuilder();
        return ub.path(id + ".txt").build().getPath();

}

@Path("prediction/")
@POST
@Consumes("text/xml")
@Produces("text/xml")
public String postPredictXml(String content,
                             @Context UriInfo uriInfo) {

        AbstractResponse resp = ResponseFactory.newXMLResponse(esttype, content);
        logger.info("postPredictXML 1");
        logger.info(resp.getXML());

        ResponseManager manager = ResponseManager.getInstance();
        String id = manager.addResponse((UnsubmittedPredictionResponse) resp);

        logger.info("postPredictXML created following xml");
        logger.info(resp.getXML());

        UriBuilder ub = uriInfo.getAbsolutePathBuilder();
        return ub.path(id + ".xml").build().getPath();
}

@Path("prediction/")
@POST
@Consumes("text/xml")
@Produces("application/json")
public String postPredictJson(String content,
                              @Context UriInfo uriInfo) {
        AbstractResponse resp = ResponseFactory.newJSONResponse(esttype, content);

        ResponseManager manager = ResponseManager.getInstance();
        String id = manager.addResponse((UnsubmittedPredictionResponse) resp);

        logger.info("Resulted JSON");
        logger.info(resp.getJSON());

        UriBuilder ub = uriInfo.getAbsolutePathBuilder();
        return ub.path(id + ".json").build().getPath();
}

@Path("prediction/{id}.txt")
@GET
@Produces("text/plain")
public String getPredictionText(@PathParam("id") String id) {
        ResponseManager manager = ResponseManager.getInstance();
        //Now the response object will be delisted from the manager class
        //so that will be garbage collected
        AbstractResponse resp = manager.getResponse(id);

        if (resp != null) {
                logger.debug("id:" + id + " \n" + resp.getTEXT());
                return resp.getTEXT();
        } else {
                logger.error("Manager returned null response for id:" + id);
                return "getting response from manager failed";
        }
}

@Path("prediction/{id}.xml")
@GET
@Produces("text/xml")
public String getPredictionXml(@PathParam("id") String id) {

        ResponseManager manager = ResponseManager.getInstance();
        //Now the response object will be delisted from the manager class
        //so that will be garbage collected
        AbstractResponse resp = manager.getResponse(id);

        if (resp != null) {
                logger.debug("id:" + id + " \n" + resp.getXML());
                return resp.getXML();
        } else {
                logger.error("Manager returned null response for id:" + id);
                return "getting response from manager failed";
        }
}

@Path("prediction/{id}.json")
@GET
@Produces("application/json")
public String getPredictionJson(@PathParam("id") String id) {
        ResponseManager manager = ResponseManager.getInstance();
        //Now the response object will be delisted from the manager class
        //so that will be garbage collected
        AbstractResponse resp = manager.getResponse(id);

        if (resp != null) {
                logger.debug("id:" + id + " \n" + resp.getJSON());
                return resp.getJSON();
        } else {
                logger.error("Manager returned null response for id:" + id);
                return "getting response from manager failed";
        }

}

@Path("karnak-client-v2.zip")
@GET
@Produces("application/octet-stream")
public File getPythonClient() {
        return new File("/home/karnak/karnak/var/www/karnak-client-v2.zip");
}

@Path("karnak-java-client-v2.zip")
@GET
@Produces("application/octet-stream")
public File getJavaClient() {
        return new File("/home/karnak/karnak/var/www/karnak-java-client-v2.zip");
}


@Path("karnak-testing.pdf")
@GET
@Produces("application/octet-stream")
public File getPDF() {
        return new File("/home/karnak/karnak/var/www/karnak-testing.pdf");
}


@Path("system/{system}/job/waiting.txt")
@GET
@Produces("text/plain")
public String getSystemText(@PathParam("system") String system) {
        system = system.replace("teragrid.org","xsede.org");
        return SubmittedJobResource.getWaitingJobsText(system);
}

@Path("system/{system}/job/waiting.html")
@GET
@Produces("text/html")
public String getSystemHtml(@PathParam("system") String system,
                            @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
        system = system.replace("teragrid.org","xsede.org");
        return SubmittedJobResource.getWaitingJobsHtml(system,true,false);
}

@Path("system/{system}/job/waiting.xml")
@GET
@Produces("text/xml")
public String getSystemXml(@PathParam("system") String system) {
        system = system.replace("teragrid.org","xsede.org");
        return SubmittedJobResource.getWaitingJobsXml(system);
}

@Path("system/{system}/job/waiting.json")
@GET
@Produces("application/json")
public String getSystemJson(@PathParam("system") String system) {
        system = system.replace("teragrid.org","xsede.org");
        return SubmittedJobResource.getWaitingJobsJson(system);
}



}
