package karnak.client.v2;


import java.text.*;
import java.util.*;
import org.w3c.dom.*;

/*
   GroupPrediction class holds prediction results for multiple queue@system.
   Please refer to the following usage:
   "usage: GroupPrediction <queue@cluster> ..<queue@cluster2> <processors> <run time (hh:mm:ss)> <NumGroups>"

   Parameter requirements
   -queue and cluster must be concatenated by @.
   -requested run time should be formatted as HH:MM:SS. If not, the program fails.
   -NumGroups MUST be equal to or greater than 1.


   Sample input:
   GroupPrediction debug@gordon.sdsc.xsede.org normal@gordon.sdsc.xsede.org normal@stampede.tacc.xsede.org  1 1:00:00 2


   GroupPrediction output:
   prediction for job generated at 2016-10-20T07:53:49Z
   processors: 1
   requested run time: 01:00:00
   submit time: 2016-10-20T07:53:49Z
   group:1	system:gordon.sdsc.xsede.org	queue:debug
   group:1	system:gordon.sdsc.xsede.org	queue:normal
   group:2	system:stampede.tacc.xsede.org	queue:normal


   Sample XML returned from Karnak v2:

   <?xml version="1.0" encoding="UTF-8" standalone="no"?>
   <QueuePredictions xmlns="http://tacc.utexas.edu/karnak/protocol/1.0" time="2016-10-20T07:53:49Z">
    <Processors>1</Processors>
    <RequestedWallTime units="minutes">60.0</RequestedWallTime>
    <Confidence>90</Confidence>
    <SubmitTime>2016-10-20T07:53:49Z</SubmitTime>
    <Group Label="1">
        <Member queue="debug" system="gordon.sdsc.xsede.org"/>
        <Member queue="normal" system="gordon.sdsc.xsede.org"/>
    </Group>
    <Group Label="2">
        <Member queue="normal" system="stampede.tacc.xsede.org"/>
    </Group>
   </QueuePredictions>

 */
public class GroupPrediction {

/* attributes applied to a query so
   all queue@system should share these attributes
 */
public int processors = -1;
public long requestedRunTimeSecs = -1;
public Date submitTime = null;
public int levelOfConfidence = -1;
public Date generatedAt = null;

/* to print out from high to low,
   I want to preserve the insertion order to the Map.
   that is why LinkedHashMap is used.
   So iterating the linkedHashMap gurantees the insertion order.
 */
private Map<String, List<PredEntry> > resMap = new LinkedHashMap<String, List<PredEntry> >();

/* PredEntry contains queue@system specific prediction results */

public static GroupPrediction fromDom(Document doc) throws KarnakException {
        GroupPrediction grpPred = new GroupPrediction();

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));

        try {
                grpPred.generatedAt = df.parse(doc.getDocumentElement().getAttribute("time"));
        } catch (ParseException e) {}

        try {
                grpPred.submitTime = df.parse(doc.getElementsByTagName("SubmitTime").item(0).getTextContent());
        } catch (ParseException e) {}

        grpPred.processors = Integer.parseInt(doc.getElementsByTagName("Processors").item(0).getTextContent());

        Element reqRunTime = (Element)doc.getElementsByTagName("RequestedWallTime").item(0);
        // assume units is minutes
        grpPred.requestedRunTimeSecs = Math.round(Float.parseFloat(reqRunTime.getTextContent()) * 60);

        grpPred.levelOfConfidence = Integer.parseInt(doc.getElementsByTagName("Confidence").item(0).getTextContent());

        NodeList groupNodes = doc.getElementsByTagName("Group");
        //if (locationNodes.getLength() > 1) {
        //        throw new KarnakException("can only handle 1 Location per document");
        //}

        for ( int index=0; index < groupNodes.getLength(); index++ ) {

                Node group = (Element)groupNodes.item(index);

                Element eElement = (Element) group;
                String label = eElement.getAttribute("Label");
                //System.out.println("fromDom Label:"+label);

                NodeList memberList = eElement.getElementsByTagName("Member");
                List<PredEntry> preds = new ArrayList<PredEntry>();

                for( int nIndex=0; nIndex < memberList.getLength(); nIndex++) {
                        Node member = memberList.item(nIndex);

                        Element memberElement = (Element) member;
                        String clusterName = memberElement.getAttribute("system");
                        String queueName = memberElement.getAttribute("queue");

                        //debug
                        //System.out.println("cluster:"+clusterName+" queue:"+queueName);

                        PredEntry entry = new PredEntry(clusterName, queueName);
                        preds.add(entry);
                }

                grpPred.resMap.put(label, preds);

        }

        return grpPred;
}

public GroupPrediction() {
}

@Override
public String toString() {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));

        StringBuilder sb = new StringBuilder(String.format("prediction for job generated at %s%n",
                                                           df.format(generatedAt)));
        sb.append( String.format("  processors: %d%n",processors));
        sb.append( String.format("  requested run time: %s%n",Util.hms(requestedRunTimeSecs)));
        sb.append( String.format("  submit time: %s%n",df.format(submitTime)));

        for( String grpName : resMap.keySet()) {
                List<PredEntry> preds = resMap.get(grpName);
                //debug
                //System.out.println("group :"+ grpName+" size:"+preds.size());
                for(PredEntry pred : preds) {
                        sb.append("group:"+grpName+"\t");
                        sb.append(pred);
                }
        }

        return sb.toString();

}

static class PredEntry {
private String clusterName;
private String queueName;


public PredEntry(){
        throw new AssertionError();
}
public PredEntry( String cluster, String queue){
        clusterName = cluster;
        queueName = queue;
}
@Override
public String toString(){
        return String.format("system:%s\tqueue:%s%n",
                             clusterName, queueName);
}
};


public static void main(String[] args) {
        if (args.length < 4) {
                System.err.println("usage: GroupPrediction <queue@cluster> ..<queue@cluster2> <processors> <run time (hh:mm:ss)> <NumGroups>");
                System.exit(1);
        }

        Karnak k = new Karnak();
        try {
                //second to the last argument is HH:MM:SS formatted string
                String hms[] = args[args.length-2].split(":");
                if ( hms.length == 3) {
                        int runTimeSecs = Integer.parseInt(hms[2])+60*(Integer.parseInt(hms[1])+60*Integer.parseInt(hms[0]));

                        List<String> queries = new ArrayList<String>();
                        for(int index=0; index < args.length-3; index++)
                                queries.add(args[index]);

                        List<String> labels = new ArrayList<String>();
                        int numGroup = Integer.parseInt(args[args.length-1]);

                        //labels MUST NOT be zero
                        if (numGroup == 0 ) {
                                System.err.println("<NumGroups> should be equal or greater than 1. Please check the number of labels. Aborting...");
                                System.exit(1);
                        }

                        for(int i=0; i < numGroup; i++)
                                labels.add(Integer.toString(i+1));

                        System.out.print(k.getGrpPredictions(Integer.parseInt(args[args.length-3]),runTimeSecs, queries, labels));
                }else{
                        System.err.println(args[args.length-1]+" is bad formatted. Aborting");
                }

        } catch (KarnakException e) {
                e.printStackTrace();
        }
}

}
