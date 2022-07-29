package karnak.service.representation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import karnak.service.predict.*;
import org.apache.commons.collections4.ListUtils;
import org.apache.log4j.Logger;
import karnak.service.WebService;
import karnak.service.predict.*;
import karnak.service.TreeNode;
import karnak.service.Table;
import karnak.service.predict.Predictable;
import karnak.service.representation.Estimation;
import karnak.service.resources.SystemsResource;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * Any grouped representation basically does not show the exact start time or
 * wait time. Therefore LabeledGroupRepr does not remember estimation type.
 *
 */
public class LabeledGroupRepr implements RepresentationStrategy {

    private static Logger logger = Logger.getLogger(LabeledGroupRepr.class.getName());
    private int numGroup;
    private List<String> labels;

    private final Estimation type;

    //Fast, medium, slow 
    public static final int DEFAULT_NUM_GROUPS = 3;
    public static final List<String> DEFAULT_LABELS = new ArrayList<String>(Arrays.asList("Fast", "Medium", "Slow"));

    //This will be moved to another class, LabeledGroupRepr
    //private List<String> labels;
    public LabeledGroupRepr(Estimation type) {
        this.type = type;
        defaultInitialization();
    }

    private void defaultInitialization() {
        numGroup = DEFAULT_NUM_GROUPS;
        labels = DEFAULT_LABELS;
    }

    /* If we were accepting two parameters, number of groups, and 
     a list of labels, the size of two parameters might not match.
     To prevent that situation, the number of groups is set to the size of 
     labels.
     
     The labels must be sorted in increasing order of wait times.
     labels.get(0) will be used to designate the group having 
     the shorted wait times.
    
     */
    public LabeledGroupRepr(Estimation type, List<String> labels) {

        this.type = type;

        if (labels == null || labels.size() == 0) {
            defaultInitialization();
        } else {
            this.numGroup = labels.size();
            this.labels = labels;
        }
        logger.debug("labels:" + labels);
    }

    public String toHTML(List<Predictable> preds) {

        if (preds == null) {
            return null;
        }

        //splitting into a few groups are done in the strategy class 

        /* This looks bad as it uses instanceof..
         However, embedding changeable part into Predictable-implemented class
         does not look good either. 
        
         Also this approach is weak to heterogenous list.
         */
        if (preds.get(0) instanceof UnsubmittedStartTimePrediction) {
            return printUnsubmittedHTML(preds);
        } else if (preds.get(0) instanceof SubmittedStartTimePrediction) {
            return printSubmittedHTML(preds);
        }

        //construct a HTML page 
        return "GroupRepr toHTML called";

    }

    public String toXML(List<Predictable> preds) {
        if (preds == null) {
            return "GroupRepr toXML called";
        } else {
            logger.debug("toXML called");
            TreeNode root = getPredictionTree(preds);
            logger.debug("TreeNode root null test:" + (root == null));

            logger.info(root.getXml());
            return root.getXml();
        }
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

        //sort predictions by their estimated time
        Collections.sort(preds);
        logger.debug("Collection sorted before split");

        /* need to split predictions into numGroup 
         Splitting a list into a few sublist is not difficult but
         handling the last sublist is dirty. 
         I decided to use partition method of ListUtils.
         */
        List<Set<Predictable>> groups = new ArrayList<Set<Predictable>>();
        int targetSize = (int) Math.ceil((double) preds.size() / numGroup);

        //little more attention to the splitting is needed
        //When the split results in empty sublist, it may be problematic
        List<List<Predictable>> partitionList = ListUtils.partition(preds, targetSize);
        logger.debug(String.format("Collection of %d elements is partitioned into %d groups whose size is %d", preds.size(), numGroup, targetSize));

        Set<Predictable> predSet = null;
        int partitionSize = partitionList.size();

        for (int i = 0; i < numGroup; i++) {
            /* 
             Set is used not to preserve the sorting sequence by the
             startTime. Within a group, randomly ordered sequence will be returned
             for grouped results.
             */
            if (i < partitionSize) {
                List<Predictable> sublist = partitionList.get(i);

                if (sublist == null || sublist.size() == 0) {
                    predSet = new HashSet<Predictable>();
                    logger.debug("sublist is empty");
                } else {
                    predSet = new HashSet<Predictable>(sublist);
                }
            } else {
                //some groups have no element due to small sample
                predSet = new HashSet<Predictable>();
                logger.debug("sublist is empty");
            }

            groups.add(predSet);
        }

        StringBuilder htmlbuilder = new StringBuilder();

        htmlbuilder.append("<title>Wait Time/Start time Predictions</title>\n");
        htmlbuilder.append("<h1 align='center'>Wait Time/Start time Predictions</h1>\n");
        htmlbuilder.append("<center>");
        
        UnsubmittedStartTimePrediction pred = (UnsubmittedStartTimePrediction) preds.get(0);

        /*requestedWallTime is in seconds.
         Need to transform it to hour and minutes. 
         */
        int hours = (int) pred.requestedWallTime / (60 * 60);
        int minutes = (int) (pred.requestedWallTime - hours * 60 * 60) / 60;

        //let's put all error messages or inadequete parameter handling to
        //PredictionResponse object creation
        //if error happens, it returns immediately
        //prepare table headers
        htmlbuilder.append("<p>Prediction for a job that will use ");
        htmlbuilder.append(pred.processors);
        htmlbuilder.append(" processing cores for ");
        htmlbuilder.append(hours);
        htmlbuilder.append(" hours ");
        htmlbuilder.append(minutes);
        htmlbuilder.append(" minutes.<br>\n\n");

        htmlbuilder.append("<p><table border='1' align='center'>\n");
        htmlbuilder.append("<tr>\n");
        htmlbuilder.append("<th>Group</th>\n");
        htmlbuilder.append("<th>Queue@System</th>\n");
        htmlbuilder.append("</tr>\n");

        //group index
        int index = 0;

        for (int i = 0; i < numGroup; i++) {
            Set<Predictable> set = groups.get(i);

            //for each row, display group information and associated queue@systems
            htmlbuilder.append("<tr>\n");
            htmlbuilder.append("<td align='right'>");
            htmlbuilder.append(labels.get(index++));
            htmlbuilder.append("</td>\n");
            htmlbuilder.append("<td align='center'>");

            //print all members in a group
            for (Predictable prediction : set) {

                pred = (UnsubmittedStartTimePrediction) prediction;

                htmlbuilder.append(pred.queue);
                htmlbuilder.append("@");
                htmlbuilder.append(pred.system);
                htmlbuilder.append("<br>\n");

            }
            //wrap up this group and go to th next group
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
        return "Not supported for this type of Job";

    }

    private String printFinishTimeHTML(List<Predictable> preds) {
        return "Not ready at this time";
    }

    public TreeNode getPredictionTree(List<Predictable> preds) {

        logger.info("getPredictionTree entered");
        if (preds.size() == 0) {
            logger.error("null prediction list is given");
            return null;
        }

        //sort predictions by their estimated time
        Collections.sort(preds);
        logger.debug("Collection sorted before split");

        /* need to split predictions into numGroup 
         Splitting a list into a few sublist is not difficult but
         handling the last sublist is dirty. 
         I decided to use partition method of ListUtils.
         */
        List<Set<Predictable>> groups = new ArrayList<Set<Predictable>>();
        int targetSize = (int) Math.ceil((double) preds.size() / numGroup);

        //little more attention to the splitting is needed
        //When the split results in empty sublist, it may be problematic
        List<List<Predictable>> partitionList = ListUtils.partition(preds, targetSize);
        logger.debug(String.format("Collection of %d elements is partitioned into %d groups whose size is %d", preds.size(), numGroup, targetSize));

        Set<Predictable> predSet = null;
        int partitionSize = partitionList.size();

        for (int i = 0; i < numGroup; i++) {
            /* 
             Set is used not to preserve the sorting sequence by the
             startTime. Within a group, randomly ordered sequence will be returned
             for grouped results.
             */
            if (i < partitionSize) {
                List<Predictable> sublist = partitionList.get(i);

                if (sublist == null || sublist.size() == 0) {
                    predSet = new HashSet<Predictable>();
                    logger.debug("sublist is empty");
                } else {
                    predSet = new HashSet<Predictable>(sublist);
                }
            } else {
                //some groups have no element due to small sample
                predSet = new HashSet<Predictable>();
                logger.debug("sublist is empty");
            }

            groups.add(predSet);
        }
        logger.info("grouping done ");

        UnsubmittedStartTimePrediction pred = (UnsubmittedStartTimePrediction) preds.get(0);

        TreeNode root = new TreeNode("QueuePredictions", null, SystemsResource.namespace);
        root.addAttribute(new TreeNode("time", WebService.dateToString(new Date())));
        root.addChild(new TreeNode("Processors", pred.processors));
        TreeNode reqWallTimeNode = new TreeNode("RequestedWallTime", pred.requestedWallTime / 60.0);
        reqWallTimeNode.addAttribute(new TreeNode("units", "minutes"));
        root.addChild(reqWallTimeNode);
        root.addChild(new TreeNode("Confidence", pred.intervalPercent));
        root.addChild(new TreeNode("SubmitTime", WebService.dateToString(new Date())));

        for (int index = 0; index < numGroup; index++) {

            Set<Predictable> set = groups.get(index);

            TreeNode groupNode = new TreeNode("Group");
            groupNode.addAttribute(new TreeNode("Label", labels.get(index)));

            //print all members in a group
            for (Predictable prediction : set) {

                pred = (UnsubmittedStartTimePrediction) prediction;

                if (pred.hasError()) {
                    StringBuilder sysInfo = new StringBuilder();
                    sysInfo.append(pred.queue);
                    sysInfo.append("@");
                    sysInfo.append(pred.system);
                    sysInfo.append(": ");
                    sysInfo.append(pred.getErrorMessage());

                    TreeNode errorNode = new TreeNode("Error", sysInfo.toString());
                    groupNode.addChild(errorNode);
                    logger.debug("system: " + pred.system + " queue:" + pred.queue + " prediction error:" + pred.getErrorMessage());
                    continue;
                }
                TreeNode memberNode = new TreeNode("Member");
                memberNode.addAttribute(new TreeNode("system", pred.system));
                memberNode.addAttribute(new TreeNode("queue", pred.queue));
                /* confidence interval is not presented on purpose
                 as we do not present the estimated time.
                 */
                groupNode.addChild(memberNode);

            }
            root.addChild(groupNode);

        }

        return root;

    }

    public String getPredictionText(List<Predictable> preds) {

        logger.info("getPredictionTree entered");
        if (preds.size() == 0) {
            logger.error("null prediction list is given");
            return null;
        }

        //sort predictions by their estimated time
        Collections.sort(preds);
        logger.debug("Collection sorted before split");

        /* need to split predictions into numGroup 
         Splitting a list into a few sublist is not difficult but
         handling the last sublist is dirty. 
         I decided to use partition method of ListUtils.
         */
        List<Set<Predictable>> groups = new ArrayList<Set<Predictable>>();
        int targetSize = (int) Math.ceil((double) preds.size() / numGroup);

        //little more attention to the splitting is needed
        //When the split results in empty sublist, it may be problematic
        List<List<Predictable>> partitionList = ListUtils.partition(preds, targetSize);
        logger.debug(String.format("Collection of %d elements is partitioned into %d groups whose size is %d", preds.size(), numGroup, targetSize));

        Set<Predictable> predSet = null;
        int partitionSize = partitionList.size();

        for (int i = 0; i < numGroup; i++) {
            /* 
             Set is used not to preserve the sorting sequence by the
             startTime. Within a group, randomly ordered sequence will be returned
             for grouped results.
             */
            if (i < partitionSize) {
                List<Predictable> sublist = partitionList.get(i);

                if (sublist == null || sublist.size() == 0) {
                    predSet = new HashSet<Predictable>();
                    logger.debug("sublist is empty");
                } else {
                    predSet = new HashSet<Predictable>(sublist);
                }
            } else {
                //some groups have no element due to small sample
                predSet = new HashSet<Predictable>();
                logger.debug("sublist is empty");
            }

            groups.add(predSet);
        }
        logger.info("grouping done ");

        UnsubmittedStartTimePrediction pred = (UnsubmittedStartTimePrediction) preds.get(0);

        Table table = null;

        //table header 
        table = new Table(type.typename());
        table.setHeader(
                "Group",
                "System",
                "Queue");

        //table body
        for (int index = 0; index < numGroup; index++) {

            Set<Predictable> set = groups.get(index);

            //print all members in a group
            for (Predictable prediction : set) {

                pred = (UnsubmittedStartTimePrediction) prediction;

                table.addRow(
                        labels.get(index),
                        pred.system,
                        pred.queue);

            }

        }

        pred = (UnsubmittedStartTimePrediction) preds.get(0);

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
