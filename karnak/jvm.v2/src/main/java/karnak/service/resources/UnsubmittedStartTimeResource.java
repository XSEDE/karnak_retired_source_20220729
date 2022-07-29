/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import javax.inject.Inject;
import javax.inject.Provider;
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

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 */
/* Multiple aliases to serve */
@Path("/karnak/v2/starttime")
public class UnsubmittedStartTimeResource {

/* Most members are similar to UnsubmittedWaitTimeResource */
private final UnsubmittedWaitTimeResource innerImpl = new UnsubmittedWaitTimeResource();

private static Logger logger = Logger.getLogger(UnsubmittedStartTimeResource.class.getName());
private final Estimation esttype = Estimation.STARTTIME;


public UnsubmittedStartTimeResource() {
}

@Path("index.html")
@GET
@Produces("text/html")
public String getSystemsHtml() {
        logger.info("index.html is called");
        return innerImpl.getSystemsHtml();
}

@Path("predict_form.html")
@GET
@Produces("text/html")
public String getPredictFormHtml() {

        logger.info("predict_form.html is called");
        return innerImpl.getPredictFormHtml();
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

        logger.info("predict.html is called");

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
public String postPredictXml(
        String content,
        @Context UriInfo uriInfo) {
        /* Not working correctly.
           if (grizzlyRequest != null) {

            String ipAddrClient = grizzlyRequest.getRemoteAddr();
            logger.debug(ipAddrClient);

            StringBuilder logBuilder = new StringBuilder();
            logBuilder.append("IP=");
            logBuilder.append(ipAddrClient);
            logBuilder.append(" Type=");
            logBuilder.append(esttype.name());
            logBuilder.append(" Format=xml");
            logger.info(logBuilder.toString());
            logger.info(content); // write received xml to log file

           }
         */

        AbstractResponse resp = ResponseFactory.newXMLResponse(esttype, content);
        logger.info(resp.getXML());

        ResponseManager manager = ResponseManager.getInstance();
        String id = manager.addResponse((UnsubmittedPredictionResponse) resp);

        logger.info("postPredictXML created xml formatted response as follows:");
        logger.info(resp.getXML());

        UriBuilder ub = uriInfo.getAbsolutePathBuilder();
        return ub.path(id + ".xml").build().getPath();
}

@Path("prediction/")
@POST
//@Consumes("application/json")
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
        return innerImpl.getPythonClient();
}


@Path("karnak-java-client-v2.zip")
@GET
@Produces("application/octet-stream")
public File getJavaClient() {
        return innerImpl.getJavaClient();
}


@Path("karnak-testing.pdf")
@GET
@Produces("application/octet-stream")
public File getPDF() {
        return new File("/home/karnak/karnak/var/www/karnak-testing.pdf");
}
/*
   @Path("system/{system}/job/waiting.txt")
   @GET
   @Produces("text/plain")
   public String getSystemText(@PathParam("system") String system) {
   system = system.replace("teragrid.org","xsede.org");
   return JobResource.getWaitingJobsText(system);
   }

   @Path("system/{system}/job/waiting.html")
   @GET
   @Produces("text/html")
   public String getSystemHtml(@PathParam("system") String system,
   @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
   system = system.replace("teragrid.org","xsede.org");
   return JobResource.getWaitingJobsHtml(system,false,true);
   }

   @Path("system/{system}/job/waiting.xml")
   @GET
   @Produces("text/xml")
   public String getSystemXml(@PathParam("system") String system) {
   system = system.replace("teragrid.org","xsede.org");
   return JobResource.getWaitingJobsXml(system);
   }

   @Path("system/{system}/job/waiting.json")
   @GET
   @Produces("application/json")
   public String getSystemJson(@PathParam("system") String system) {
   system = system.replace("teragrid.org","xsede.org");
   return JobResource.getWaitingJobsJson(system);
   }
 */
}
