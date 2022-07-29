/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import karnak.service.representation.Estimation;
import karnak.service.representation.StrategyFactory;
import karnak.service.representation.*;
import karnak.service.predict.UnsubmittedStartTimeQuery;
import karnak.service.predict.SubmittedStartTimeQuery;
import karnak.service.response.UnsubmittedPredictionResponse;
import java.util.List;
import javax.ws.rs.WebApplicationException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.log4j.Logger;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * This class creates a prediction response object for unsubmitted/submitted
 * jobs. Unsubmitted jobs are hypothetical jobs that have not been submitted to
 * the any queue. Users may be interested in comparing estimated wait times
 * between multiple queue@systems for jobs that are not submitted yet.
 *
 * The wait time predictions for jobs that had been submitted but not yet
 * started can be queried by SubmittedPredictionResponse.
 *
 *
 */
public class ResponseFactory {

    private static Logger logger = Logger.getLogger(ResponseFactory.class.getName());

    /*
     Returns predicted wait times for unsubmitted jobs. 
    
     This static method returns a prediction output object that has 
     predicted wait times/start times of multiple queue@system.
    
     "coresStr" contains the number of processors required to run jobs.
     "hour" and "minutes" are used to compute the requested wall time.
     "queueAtSystems" must be a list of String that is formatted "queue@system". 
    
     How to represent prediction results are determined by three parameters.
     First the "predictionFormat" string specifies the type of the representation.
     1. "RawRepr" returns the predicted times as they are. 
     2. "GroupRepr" returns grouped wait times to the client. Systems in the same
     group are expected to have similar wait time/start time. 
     They are indexed ordinally from 1 to number of groups. 
     The minimal number of group should be equal or greater than 2. 
     Otherwise the default number of group, 3, is used for grouping predictions.
     The labels parameter is not used for "GroupRepr".
    
     3. "LabeledGroupRepr" are basically same as "GroupRepr" but the client can
     label the groups by supplying the list of group names. 
     The number of group parameter is ignored when the labels is provided.
    
     */
    public static AbstractResponse newResponse(
            Estimation type,
            List<String> queueAtSystems,
            String coresStr /* Number of processors in string */,
            String hours,
            String minutes,
            String confidence,
            String predictionFormat /* "RawRepr", "GroupRepr","LabeledGroupRepr", ...*/,
            int numGroup /* minium should be equal to or greater than 2 
             for GroupRepr/LabeledGroupRepr */,
            List<String> labels) {

        AbstractResponse response = null;
        List<UnsubmittedStartTimeQuery> queries = new ArrayList<UnsubmittedStartTimeQuery>();

        for (String queueAtSystem : queueAtSystems) {
            //use builder pattern
            //The inner class of UnsubmittedStartTimeQuery will 
            UnsubmittedStartTimeQuery query = new UnsubmittedStartTimeQuery.Builder(queueAtSystem, coresStr).walltime(hours, minutes).interval(confidence).build();

            //if inputs are problematic, set error message and return
            //in case of errors, getHTML(), getXML(), getJSON() return error message
            //and do not format the predictions
            /*
             if (validateInputs() == false) {
             response = new UnsubmittedPredictionResponse();
             response.setRepresentation(StrategyFactory.newInstance(predictionFormat, numGroup, labels));
             return response;
             }
             */
            //clean up the parameters 
            //generate a query object ..
            //how do we know submitted or unsubmitted query ?
            //    public UnsubmittedPredictionResponse(){
            queries.add(query);

        }

        response = new UnsubmittedPredictionResponse(queries);
        logger.debug("response object created");

        //set the way to represent the prediction output
        response.setReprestation(StrategyFactory.newInstance(type, predictionFormat, numGroup, labels));
        logger.debug("strategy set");

        return response;
        //create response object 
    }

    /*
     Returns predicted wait times for submitted jobs. 
     In other words, this static method generates a prediction for the jobs that
     have been already submitted but have not executed yet, thus are in the queues.
     
     This static method returns a prediction output object that has 
     predicted wait times/start times for the given system and job Ids.
    
     The necessary parameters for estimation are 
     1) system name, 
     2) job id, 
     3) confidence level String.
     The default confidence level is set to 90%.
     "jobIdAtSystems" must be a list of String that is formatted "jobId@system".
    
     How to represent prediction results are determined by three parameters.
     First the "predictionFormat" string specifies the type of the representation.
     1. "RawRepr" returns the predicted times as they are. 
     2. "GroupRepr" returns grouped wait times to the client. Systems in the same
     group are expected to have similar wait time/start time. 
     They are indexed ordinally from 1 to number of groups. 
     The minimal number of group should be equal or greater than 2. 
     Otherwise the default number of group, 3, is used for grouping predictions.
     The labels parameter is not used for "GroupRepr".
    
     3. "LabeledGroupRepr" are basically same as "GroupRepr" but the client can
     label the groups by supplying the list of group names. 
     The number of group parameter is ignored when the labels is provided.
    
     */
    public static AbstractResponse newResponse(
            Estimation type,
            List<String> jobIdAtSystems,
            String confidence,
            String predictionFormat /* "RawRepr", "GroupRepr","LabeledGroupRepr", ...*/,
            int numGroup /* minium should be equal to or greater than 2 
             for GroupRepr/LabeledGroupRepr */,
            List<String> labels) {

        //clean up the parameters 
        //generate a query object ..
        //how do we know submitted or unsubmitted query ?
        SubmittedStartTimeQuery query = new SubmittedStartTimeQuery();

        //    public UnsubmittedPredictionResponse(){
        ArrayList<SubmittedStartTimeQuery> queries = null;

        queries.add(query);

        AbstractResponse response = new SubmittedPredictionResponse(queries); //get strategy

        //set the way to represent the prediction output
        response.setReprestation(StrategyFactory.newInstance(type, predictionFormat, numGroup, labels));

        return response;
        //create response object 
    }

    /* The XML must contain the following items: 
       
     <Predictions>
     <Processors>1</Processors>
     <RequestedWallTime units='minutes'>15</RequestedWallTime>
     <Confidence>90</Confidence>
     <Location system='system' queue='queue'/>
     <Location system='system' queue='queue'/>
     <Format>Raw </Format>  // Can be Raw, Group, LabeledGroup
     <NumGroups>3 </NumGroups> // ignored for Raw format. 
     <Labels>Fast,medium,Slow</Labels> //If set, the format is set to LabeledGroup
     </Predictions>
	
     */
    public static AbstractResponse newXMLResponse(Estimation type, String xmlString) {

        AbstractResponse response = null;
        List<UnsubmittedStartTimeQuery> queries = new ArrayList<UnsubmittedStartTimeQuery>();

        //extract location information 
        logger.info(xmlString);

        int processors = 0;
        int wallTimeSecs = 0;
        String predFormat = "LabeledGroup";
        int numGroups = 3;

        DocumentBuilder builder = null;
        try {
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            logger.error("failed to create factory: " + e.getMessage());
            throw new WebApplicationException(500);
        } catch (Exception ex) {
            logger.error("Exception creating DocumentBuilder");
            ex.printStackTrace();
            throw new WebApplicationException(500);
        }
        Document doc = null;
        try {
            doc = builder.parse(new ByteArrayInputStream(xmlString.getBytes()));
        } catch (IOException e) {
            logger.error("failed to read request: " + e.getMessage());
            throw new WebApplicationException(400);
        } catch (SAXException e) {
            logger.error("failed to parse request: " + e.getMessage());
            throw new WebApplicationException(400);
        }

        NodeList list = doc.getElementsByTagName("Processors");
        if (list.getLength() == 0) {
            logger.warn("didn't find Processors");
            throw new WebApplicationException(400);
        }
        String text = ((Text) (list.item(0).getFirstChild())).getWholeText();
        try {
            processors = Integer.parseInt(text);
        } catch (Exception e) {
            throw new WebApplicationException(400);
        }

        list = doc.getElementsByTagName("RequestedWallTime");
        if (list.getLength() == 0) {
            logger.warn("didn't find RequestedWallTime");
            throw new WebApplicationException(400);
        }
        text = ((Text) (list.item(0).getFirstChild())).getWholeText();
        try {
            wallTimeSecs = Integer.parseInt(text) * 60; // assuming XML has minutes
        } catch (Exception e) {
            throw new WebApplicationException(400);
        }

        int confidence = 90;
        list = doc.getElementsByTagName("Confidence");
        if (list.getLength() == 0) {
            logger.warn("using default Confidence");
        } else {
            text = ((Text) (list.item(0).getFirstChild())).getWholeText();
            try {
                confidence = Integer.parseInt(text);
            } catch (Exception e) {
                throw new WebApplicationException(400);
            }
        }

        //prediction format 
        list = doc.getElementsByTagName("Format");
        if (list.getLength() == 0) {
            logger.warn("using default format");
        } else {
            text = ((Text) (list.item(0).getFirstChild())).getWholeText();
            try {
                predFormat = text;
                logger.info("predFormat:" + predFormat);
            } catch (Exception e) {
                throw new WebApplicationException(400);
            }
        }

        // number of groups
        list = doc.getElementsByTagName("NumGroups");
        if (list.getLength() == 0) {
            logger.warn("using default NumGroups");
        } else {
            text = ((Text) (list.item(0).getFirstChild())).getWholeText();
            try {
                numGroups = Integer.parseInt(text);
                logger.info("numGroups:" + numGroups);
            } catch (Exception e) {
                throw new WebApplicationException(400);
            }
        }

        // Labels can be null 
        List<String> labels = null;
        list = doc.getElementsByTagName("Labels");
        if (list.getLength() == 0) {
            logger.warn("using default Labels");
        } else {

            Node labelNode = list.item(0).getFirstChild();

            //if users supply nothing, ignore that field
            if (labelNode != null) {

                text = ((Text) labelNode).getWholeText();
                try {
                    logger.debug("Labels: " + text);
                    labels = Arrays.asList(text.split(","));
                    logger.debug("Labels size:" + labels.size());

                } catch (Exception ex) {
                    throw new WebApplicationException(400);
                }
            }
        }

        list = doc.getElementsByTagName("Location");
        if (list.getLength() == 0) {
            logger.warn("didn't find Location");
            throw new WebApplicationException(400);
        }
        for (int i = 0; i < list.getLength(); i++) {

            String system = ((Attr) (list.item(i).getAttributes().getNamedItem("system"))).getValue();
            if (system == null) {
                logger.warn("didn't find system attribute of Location");
                throw new WebApplicationException(400);
            }
            String queue = ((Attr) (list.item(i).getAttributes().getNamedItem("queue"))).getValue();
            if (queue == null) {
                logger.warn("didn't find queue attribute of Location");
                throw new WebApplicationException(400);
            }

            //use builder pattern
            //The inner class of UnsubmittedStartTimeQuery will 
            UnsubmittedStartTimeQuery query = new UnsubmittedStartTimeQuery.Builder(queue, system, processors).walltime(wallTimeSecs).interval(confidence).build();
            logger.debug(query);
            queries.add(query);

        }

        response = new UnsubmittedPredictionResponse(queries);
        logger.debug("response object created");

        //set the way to represent the prediction output
        //String predictionFormat, int numGroup, List<String> labels
        response.setReprestation(StrategyFactory.newInstance(type, predFormat, numGroups, labels));
        logger.debug(response.getXML());
        return response;
    }

    public static AbstractResponse newJSONResponse(Estimation type, String jsonString) {
        //The response will be the same. Just call 
        return newXMLResponse(type, jsonString);
    }

    public static AbstractResponse newTextResponse(Estimation type, String txtString) {

        logger.info(txtString);
        AbstractResponse response = null;
        List<UnsubmittedStartTimeQuery> queries = new ArrayList<UnsubmittedStartTimeQuery>();
        /*
         sample input of txtString
        
         processors 1
         requestedWallTime hh:mm:ss
         confidence 90
         location system queue
         location system queue
         */
        int processors = 0;
        int wallTimeSecs = 0;
        String predFormat = "LabeledGroup";
        int numGroups = 3;
        int confidence = 90;
        List<String> labels = null;

        String[] lines = txtString.split("\n");
        for (String line : lines) {
            // Jungha 02/26/2016
            // left, right trim is added 
            String[] tok = line.trim().split("\\s");
            if (tok.length == 0) {
                continue;
            }
            if (tok[0].equalsIgnoreCase("processors")) {
                if (tok.length != 2) {
                    throw new WebApplicationException(400);
                }
                try {
                    processors = Integer.parseInt(tok[1]);
                } catch (NumberFormatException e) {
                    logger.error("processors isn't an integer: " + tok[1]);
                    throw new WebApplicationException(400);
                }
            } else if (tok[0].equalsIgnoreCase("requestedWallTime")) {
                if (tok.length != 2) {
                    logger.error("wrong number of parameters for requested wall time: " + tok.length);
                    throw new WebApplicationException(400);
                }
                String[] tok2 = tok[1].split(":");
                if (tok2.length != 3) {
                    logger.error("wrong number of tokens for requested wall time: " + tok[1]);
                    throw new WebApplicationException(400);
                }
                try {
                    wallTimeSecs = Integer.parseInt(tok2[2])
                            + 60 * (Integer.parseInt(tok2[1]) + 60 * Integer.parseInt(tok2[0]));
                } catch (NumberFormatException e) {
                    logger.error("requested wall time tokens aren't integers: " + tok[1]);
                    throw new WebApplicationException(400);
                }
            } else if (tok[0].equalsIgnoreCase("confidence")) {
                if (tok.length != 2) {
                    logger.error("wrong number of parameters for confidence: " + tok.length);
                    throw new WebApplicationException(400);
                }
                try {
                    confidence = Integer.parseInt(tok[1]);
                } catch (NumberFormatException e) {
                    logger.error("confidence isn't an integer: " + tok[1]);
                    throw new WebApplicationException(400);
                }
            } else if (tok[0].equalsIgnoreCase("location")) {
                if (tok.length != 3) {
                    logger.error("wrong number of parameters for location: " + tok.length);
                    throw new WebApplicationException(400);
                }
                /* Assumes that other parameters are already ready 
                 when the location information is being read.
                 Therefore we simply look up other variables to instantiate 
                 UnsubmittedStartTimeQuery object. 
                
                 */
                //use builder pattern
                //The inner class of UnsubmittedStartTimeQuery will 
                String system = tok[1];
                String queue = tok[2];

                UnsubmittedStartTimeQuery query = new UnsubmittedStartTimeQuery.Builder(queue, system, processors).walltime(wallTimeSecs).interval(confidence).build();
                logger.debug(query);
                queries.add(query);

            } else if (tok[0].equalsIgnoreCase("Format")) {
                if (tok.length != 2) {
                    logger.error("wrong number of parameters for prediction format: " + tok.length);
                    throw new WebApplicationException(400);
                }

                predFormat = tok[1];
                logger.info("predFormat:" + predFormat);

            } else if (tok[0].equalsIgnoreCase("NumGroups")) {
                if (tok.length != 2) {
                    logger.error("wrong number of parameters for number of Groups: " + tok.length);
                    throw new WebApplicationException(400);
                }
                try {
                    numGroups = Integer.parseInt(tok[1]);
                } catch (NumberFormatException e) {
                    logger.error("numGroups isn't an integer: " + tok[1]);
                    throw new WebApplicationException(400);
                }
            } else if (tok[0].equalsIgnoreCase("Labels")) {
                if (tok.length != 2) {
                    /* The RawRepr does not need the labels list so 
                     throwing an exception looks not good. 
                     let's create an empty list for labels and go on.
                     */
                    if (predFormat.equalsIgnoreCase("Raw") || predFormat.equalsIgnoreCase("Group")) {
                        logger.info("wrong number of parameters for labels. Empty labels are created.");
                        labels = Collections.<String>emptyList();
                        continue;
                    } else {
                        //LabeledGroupRepr MUST provide the list of labels
                        throw new WebApplicationException(400);
                    }
                }

                String strlabels = tok[1];
                try {
                    logger.debug("Labels: " + strlabels);
                    labels = Arrays.asList(strlabels.split(","));
                    logger.debug("Labels size:" + labels.size());

                } catch (Exception ex) {
                    throw new WebApplicationException(400);
                }

            } else {
                logger.error("unexpected parameter: " + tok[0]);
                throw new WebApplicationException(400);
            }
        }

        response = new UnsubmittedPredictionResponse(queries);
        logger.debug("response object created");

        //set the way to represent the prediction output
        //String predictionFormat, int numGroup, List<String> labels
        response.setReprestation(StrategyFactory.newInstance(type, predFormat, numGroups, labels));
        logger.debug(response.getXML());
        return response;
    }

}
