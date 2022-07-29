package karnak.service.representation;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.ws.rs.WebApplicationException;

import karnak.service.WebService;
import karnak.service.TreeNode;
import karnak.service.Table;
import karnak.service.predict.Predictable;
import karnak.service.predict.UnsubmittedStartTimePrediction;
import karnak.service.predict.SubmittedStartTimePrediction;
import karnak.service.representation.Estimation;
import karnak.service.util.SystemInfoQuery;
import karnak.service.resources.SystemsResource;
import org.apache.log4j.Logger;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wooj
 *
 * Raw Representation returns both start time and wait time as they are just
 * another form of each other.
 *
 *
 * As RawRepr does not have member data, it is made to singleton static factory.
 *
 *
 */
public class RawRepr implements RepresentationStrategy {

    /* This class was written as singleton factory but 
     changed to normal instance.
 
     //private static final RawRepr INSTANCE =  new RawRepr();
     //private RawRepr(){}    
     //returns same object reference
     //public static RawRepr getInstance(){ return INSTANCE;}
     */
    private static Logger logger = Logger.getLogger(RawRepr.class.getName());

    private final Estimation type;

    public RawRepr(Estimation type) {
        this.type = type;
    }

    public String toHTML(List<Predictable> preds) {

        if (preds == null) {
            logger.info("No prediction result is provided");
            return "No prediction result is provided.";
        }

        //sort predictions by their estimated time
        Collections.sort(preds);

        if (preds.get(0) instanceof UnsubmittedStartTimePrediction) {
            return printUnsubmittedHTML(preds);
        } else if (preds.get(0) instanceof SubmittedStartTimePrediction) {
            return printSubmittedHTML(preds);
        }

        return "Unknown type: Objects cannot be represented by raw representation.";

    }

    public String toXML(List<Predictable> preds) {
        if (preds == null) {
            return "RawRepr toXML called";
        }

        Collections.sort(preds);

        if (preds.get(0) instanceof UnsubmittedStartTimePrediction) {
            TreeNode root = getPredictionTree(preds);
            logger.info(root.getXml());
            return root.getXml();
        } else if (preds.get(0) instanceof SubmittedStartTimePrediction) {
            return "Not yet supported";
        }

        return "Unsupported operation";
    }

    public String toJSON(List<Predictable> preds) {
        if (preds == null) {
            return null;
        }

        Collections.sort(preds);

        if (preds.get(0) instanceof UnsubmittedStartTimePrediction) {
            TreeNode root = getPredictionTree(preds);
            logger.info(root.getJson());
            return root.getJson();
        } else if (preds.get(0) instanceof SubmittedStartTimePrediction) {
            return "Not yet supported";
        }
        return "Unknown prediction type";
    }

    public String toTEXT(List<Predictable> preds) {

        if (preds == null) {
            logger.info("No prediction result is provided");
            return "No prediction result is provided";
        }

        //sort predictions by their estimated time
        Collections.sort(preds);

        if (preds.get(0) instanceof UnsubmittedStartTimePrediction) {
            return getPredictionText(preds);
        } else if (preds.get(0) instanceof SubmittedStartTimePrediction) {
            return "Not yet supported";
        }

        return "Unknown type: Objects cannot be represented by raw representation.";

    }

    private String printUnsubmittedHTML(List<Predictable> preds) {

        UnsubmittedStartTimePrediction pred = (UnsubmittedStartTimePrediction) preds.get(0);

        StringBuilder htmlbuilder = new StringBuilder();

        htmlbuilder.append("<title>Wait Time/Start time Predictions</title>\n");
        htmlbuilder.append("<h1 align='center'>Wait Time/Start time Predictions</h1>\n");

        
        /*requestedWallTime is in seconds.
          Need to transform it to hour and minutes. 
        */
        
        int hours = (int)pred.requestedWallTime/ (60 * 60);
        int minutes = (int) (pred.requestedWallTime - hours * 60 * 60) / 60;

        
        //let's put all error messages or inadequete parameter handling to
        //PredictionResponse object creation
        //if error happens, it returns immediately
        //prepare table headers
        htmlbuilder.append("<center>");
        htmlbuilder.append("Prediction for a job that will use ");
        htmlbuilder.append(pred.processors);
        htmlbuilder.append(" processing cores for ");
        htmlbuilder.append(hours);
        htmlbuilder.append(" hours ");
        htmlbuilder.append(minutes);                
        htmlbuilder.append(" minutes.<br>\n\n");
        htmlbuilder.append("<p><table border='1' align='center'>\n");
        htmlbuilder.append("<tr>\n");
        htmlbuilder.append("<th>System</th>\n");
        htmlbuilder.append("<th>Queue</th>\n");

        htmlbuilder.append("<th>");
        htmlbuilder.append(type.typename());
        htmlbuilder.append("<br>");
        htmlbuilder.append(type.format());
        htmlbuilder.append("</th>");

        htmlbuilder.append("<th>");
        htmlbuilder.append("Estimated finish time");
        htmlbuilder.append("</th>");

        htmlbuilder.append("<th>");
        htmlbuilder.append(pred.intervalPercent);
        htmlbuilder.append("% confidence <br> (hours:minutes:seconds)</th>\n");
        htmlbuilder.append("</tr>\n");

        Date curTime = new Date();
        //create rows
        for (Predictable prediction : preds) {

            pred = (UnsubmittedStartTimePrediction) prediction;

            /*
             else if (pred instanceof SubmittedStartTimePrediction )
             SubmittedStartTimePrediction prediction = (UnsubmittedStartTimePrediction) pred;
             */
            htmlbuilder.append("<tr>\n");
            htmlbuilder.append("<td align='right'>");
            htmlbuilder.append(pred.system);
            htmlbuilder.append("</td>\n");
            htmlbuilder.append("<td align='center'>");
            htmlbuilder.append(pred.queue);
            htmlbuilder.append("</td>\n");

            //print N/A if the job cannot be run on the requested system
            //however, this may have already been prevented from 
            //prediction. 
            if ((pred.processors > SystemInfoQuery.getMaxProcessors(pred.system, pred.queue))
                    || (pred.requestedWallTime > SystemInfoQuery.getMaxWallTime(pred.system, pred.queue))) {

                htmlbuilder.append("<td align='right'>N/A</td>\n");
                htmlbuilder.append("<td align='center'>N/A</td>\n");
                htmlbuilder.append("<td align='right'>N/A</td>\n");
                htmlbuilder.append("</tr>\n");

                continue;
            }

            switch (type) {
                case STARTTIME:
                    //start time field
                    htmlbuilder.append("<td align='center'>");
                    htmlbuilder.append(WebService.dateToLocalString(pred.startTime));
                    htmlbuilder.append("</td>\n");
                    break;
                case WAITTIME:
                    // compute wait time 

                    int waitTime = (int) ((pred.startTime.getTime() - curTime.getTime()) / 1000);
                    if (waitTime < 0) {
                        waitTime = 0;
                    }

                    htmlbuilder.append("<td align='right'>");
                    htmlbuilder.append(WebService.hms(waitTime));
                    htmlbuilder.append("</td>\n");
                    break;

            }

            //display finish time 
            htmlbuilder.append("<td align='right'>");
            htmlbuilder.append(WebService.dateToLocalString(pred.getFinishTime()));
            htmlbuilder.append("</td>\n");

            htmlbuilder.append("<td align = 'right'>&plusmn;");
            htmlbuilder.append(WebService.hms((int) pred.intervalSecs));
            htmlbuilder.append("</td>\n");
            htmlbuilder.append("</tr>\n");

        }

        htmlbuilder.append("</table><br><br>\n");

        //print out error messages if any prediction has error
        for (Predictable prediction : preds) {

            pred = (UnsubmittedStartTimePrediction) prediction;

            /* prediction error can come from various reasons
             so instead of checking null start time, 
             checking error message will be better.
             */
            if (pred.hasError()) {
                htmlbuilder.append("Estimation for the ");
                htmlbuilder.append(pred.queue);
                htmlbuilder.append("@");
                htmlbuilder.append(pred.system);
                htmlbuilder.append(" cannot be made because :\n");
                htmlbuilder.append(pred.getErrorMessage());
                htmlbuilder.append("<br><br>\n");
            }
        }
        htmlbuilder.append("</center>");

        return htmlbuilder.toString();
    }

    private String printSubmittedHTML(List<Predictable> preds) {
        return "Not ready at this time";

    }

    public TreeNode getPredictionTree(List<Predictable> preds) {

        if (preds.size() == 0) {
            logger.error("Null prediction list is provided");
            return null;
        }

        UnsubmittedStartTimePrediction pred = (UnsubmittedStartTimePrediction) preds.get(0);

        TreeNode root = new TreeNode("QueuePredictions", null, SystemsResource.namespace);
        root.addAttribute(new TreeNode("time", WebService.dateToString(new Date())));
        root.addChild(new TreeNode("Processors", pred.processors));
        TreeNode reqWallTimeNode = new TreeNode("RequestedWallTime", pred.requestedWallTime / 60.0);
        reqWallTimeNode.addAttribute(new TreeNode("units", "minutes"));
        root.addChild(reqWallTimeNode);
        root.addChild(new TreeNode("Confidence", pred.intervalPercent));
        root.addChild(new TreeNode("SubmitTime", WebService.dateToString(new Date())));

        for (Predictable prediction : preds) {

            pred = (UnsubmittedStartTimePrediction) prediction;

            TreeNode locationNode = new TreeNode("Location");
            locationNode.addAttribute(new TreeNode("system", pred.system));
            locationNode.addAttribute(new TreeNode("queue", pred.queue));

            /* prediction error can come from various reasons
             so instead of checking null start time, 
             checking error message will be better.
             */
            if (pred.hasError()) {
                StringBuilder sysInfo = new StringBuilder();
                sysInfo.append(pred.queue);
                sysInfo.append("@");
                sysInfo.append(pred.system);
                sysInfo.append(": ");
                sysInfo.append(pred.getErrorMessage());

                TreeNode errorNode = new TreeNode("Error", sysInfo.toString());
                locationNode.addChild(errorNode);

            } else {

                switch (type) {
                    case STARTTIME:
                        //start time field
                        locationNode.addChild(new TreeNode("StartTime", WebService.dateToString(pred.startTime)));
                        break;
                    case WAITTIME:
                        long waitTimeSecs = (pred.startTime.getTime() - new Date().getTime()) / 1000;
                        TreeNode wtNode = new TreeNode("WaitTime", waitTimeSecs / 60.0);
                        wtNode.addAttribute(new TreeNode("units", "minutes"));
                        locationNode.addChild(wtNode);
                        break;
                }

                TreeNode confNode
                        = new TreeNode("Confidence", pred.intervalSecs / 60.0);
                confNode.addAttribute(new TreeNode("units", "minutes"));
                locationNode.addChild(confNode);
            }
            root.addChild(locationNode);
        }

        return root;
    }

    public String getPredictionText(List<Predictable> preds) {

        if (preds.size() == 0) {
            logger.error("Null prediction list is provided");
            return null;
        }

        UnsubmittedStartTimePrediction pred = (UnsubmittedStartTimePrediction) preds.get(0);

        Table table = null;

        StringBuilder typeColumnNamebuilder = new StringBuilder();
        typeColumnNamebuilder.append(type.typename());
        typeColumnNamebuilder.append("\n(");
        typeColumnNamebuilder.append(type.format());
        typeColumnNamebuilder.append(")");

        switch (type) {

            case STARTTIME:
                table = new Table("Start Time Predictions");

                table.setHeader("System",
                        "Queue",
                        typeColumnNamebuilder.toString(),
                        pred.intervalPercent + "% Confidence\n(hours:minutes:seconds)");
                break;

            case WAITTIME:
                table = new Table("Wait Time Predictions");
                table.setHeader("System",
                        "Queue",
                        typeColumnNamebuilder.toString(),
                        pred.intervalPercent + "% Confidence\n(hours:minutes:seconds)");

                break;

        }

        Date curTime = new Date();

        for (Predictable prediction : preds) {

            pred = (UnsubmittedStartTimePrediction) prediction;

            /* prediction error can come from various reasons
             so instead of checking null start time, 
             checking error message will be better.
             */
            if (pred.hasError()) {
                table.addRow(pred.system, pred.queue, "N/A", "N/A");
                continue;

            } else {

                switch (type) {
                    case STARTTIME:
                        //start time field
                        table.addRow(pred.system,
                                pred.queue,
                                WebService.dateToLocalString(pred.startTime),
                                WebService.hms((int) pred.intervalSecs));
                        break;
                    case WAITTIME:

                        int waitTime = (int) ((pred.startTime.getTime() - curTime.getTime()) / 1000);
                        int confInt = (int) pred.intervalSecs;
                        logger.info("  wait time prediction for " + pred.queue + "@" + pred.system + " is "
                                + waitTime + " secs +- " + confInt);
                        table.addRow(pred.system, pred.queue,
                                WebService.hms(waitTime), WebService.hms(confInt));
                        break;
                }

            }
        }

        pred = (UnsubmittedStartTimePrediction) preds.get(0);
        String text = "";

        int hours = (int) pred.intervalSecs / (60 * 60);
        int minutes = (int) (pred.intervalSecs - hours * 60 * 60) / 60;

        StringBuilder textBuilder = new StringBuilder();
        textBuilder.append("\nPrediction for a job submitted at ");
        textBuilder.append(WebService.dateToLocalString(new Date()));
        textBuilder.append(" ");
        textBuilder.append(WebService.getLocalTimeZoneString());
        textBuilder.append(" that will\n");
        textBuilder.append("    use ");
        textBuilder.append(pred.processors);
        textBuilder.append(" processing cores\n");
        textBuilder.append("    for ");
        textBuilder.append(hours);
        textBuilder.append(" hours and ");
        textBuilder.append(minutes);
        textBuilder.append(" minutes.\n\n");

        //print out error message before the table
        //because there's no column for displaying errors in the table
        for (Predictable prediction : preds) {

            pred = (UnsubmittedStartTimePrediction) prediction;

            /* prediction error can come from various reasons
             so instead of checking null start time, 
             checking error message will be better.
             */
            if (pred.hasError()) {
                textBuilder.append("Estimation for the ");
                textBuilder.append(pred.queue);
                textBuilder.append("@");
                textBuilder.append(pred.system);
                textBuilder.append(" cannot be made because :\n");
                textBuilder.append(pred.getErrorMessage());
                textBuilder.append("\n\n");
            }
        }

        textBuilder.append(table.getText());

        logger.debug(textBuilder.toString());
        return textBuilder.toString();
    }

}
