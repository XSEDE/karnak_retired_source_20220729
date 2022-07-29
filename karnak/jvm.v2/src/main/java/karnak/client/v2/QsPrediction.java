package karnak.client.v2;


import java.text.*;
import java.util.*;
import org.w3c.dom.*;

/*
   QsPrediction class holds multple prediction results in raw format.

   Please refer to the following usage:
   "usage:    QsPrediction <queue@cluster> ..<queue@cluster2> <processors> <run time (hh:mm:ss)>


   Parameter requirements
   -queue and cluster must be concatenated by @.
   -requested run time should be formatted as HH:MM:SS. If not, the program fails.


   Sample input:
   QsPrediction normal@gordon.sdsc.xsede.org debug@gordon.sdsc.xsede.org  1 01:00:00

   QsPrediction output:
   prediction for job generated at 2016-10-18T04:52:54Z
     processors: 1
     requested run time: 01:00:00
     submit time: 2016-10-18T04:52:54Z
     system: gordon.sdsc.xsede.org  queue:normal
     predicted start time: 2016-10-18T04:53:09Z +- 00:01:24
               (wait time:             00:00:15 +- 00:01:24)
     system: gordon.sdsc.xsede.org  queue:debug
     predicted start time: 2016-10-18T04:53:09Z +- 00:01:24
               (wait time:             00:00:15 +- 00:01:24)

 */

public class QsPrediction {
public int processors = -1;
public long requestedRunTimeSecs = -1;
public Date submitTime = null;
public int levelOfConfidence = -1;
public Date generatedAt = null;
/* PredEntry contains queue@system specific prediction results */
private List<PredEntry> preds = new ArrayList<PredEntry>();

public static QsPrediction fromDom(Document doc) throws KarnakException {
        QsPrediction qs = new QsPrediction();

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));

        try {
                qs.generatedAt = df.parse(doc.getDocumentElement().getAttribute("time"));
        } catch (ParseException e) {}

        try {
                qs.submitTime = df.parse(doc.getElementsByTagName("SubmitTime").item(0).getTextContent());
        } catch (ParseException e) {}

        qs.processors = Integer.parseInt(doc.getElementsByTagName("Processors").item(0).getTextContent());

        Element reqRunTime = (Element)doc.getElementsByTagName("RequestedWallTime").item(0);
        // assume units is minutes
        qs.requestedRunTimeSecs = Math.round(Float.parseFloat(reqRunTime.getTextContent()) * 60);

        qs.levelOfConfidence = Integer.parseInt(doc.getElementsByTagName("Confidence").item(0).getTextContent());

        NodeList locationNodes = doc.getElementsByTagName("Location");
        //if (locationNodes.getLength() > 1) {
        //        throw new KarnakException("can only handle 1 Location per document");
        //}

        for ( int index=0; index < locationNodes.getLength(); index++ ) {
                Element location = (Element)locationNodes.item(index);

                String clusterName = location.getAttribute("system");
                String queueName = location.getAttribute("queue");
                Date predictedStartTime = null;
                long predictedWaitTimeSecs = 0;
                long confidenceIntervalSecs =0;
                try {
                        predictedStartTime = df.parse(location.getElementsByTagName("StartTime").item(0).getTextContent());
                        predictedWaitTimeSecs = Math.round( (predictedStartTime.getTime() - qs.generatedAt.getTime()) / 1000);
                        //System.out.println("location["+index+"] " +queueName+"@"+clusterName+": start_time:"+predictedStartTime+" waitsec:"+predictedWaitTimeSecs);

                } catch (NullPointerException e) {
                        // no StartTime element, but that's ok - there will be a WaitTime one
                } catch (ParseException e) {
                }

                Element waitTime = (Element)location.getElementsByTagName("WaitTime").item(0);
                try {
                        // assume units is minutes
                        predictedWaitTimeSecs = Math.round(Float.parseFloat(waitTime.getTextContent()) * 60);
                        predictedStartTime = new Date(qs.generatedAt.getTime() + predictedWaitTimeSecs * 1000);
                        //System.out.println("location["+index+"] " +queueName+"@"+clusterName+": wait_time:"+predictedWaitTimeSecs+" starttime:"+predictedStartTime);

                } catch (NullPointerException e) {
                        // no WaitTime element, but that's ok - there should have been a StartTime one
                }
                Element conf = (Element)location.getElementsByTagName("Confidence").item(0);
                if (conf.getAttribute("units").equals("minutes")) {
                        confidenceIntervalSecs = Math.round(Float.parseFloat(conf.getTextContent()) * 60);
                }

                PredEntry entry = new PredEntry(clusterName, queueName, predictedStartTime, predictedWaitTimeSecs,confidenceIntervalSecs);
                qs.preds.add(entry);

        }





        return qs;
}

public QsPrediction() {
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
        for(PredEntry pred : preds) {
                sb.append(pred);
        }

        return sb.toString();

}

static class PredEntry {
private String clusterName;
private String queueName;
private Date predictedStartTime = null;
private long predictedWaitTimeSecs = -1;
private long confidenceIntervalSecs = -1;

public PredEntry(){
        throw new AssertionError();
}
public PredEntry(String cluster, String queue, Date start, long waitsecs, long confsecs){
        clusterName = cluster;
        queueName = queue;
        predictedStartTime = new Date(start.getTime());
        predictedWaitTimeSecs = waitsecs;
        confidenceIntervalSecs = confsecs;
}
@Override
public String toString(){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("GMT"));

        String str= String.format("  system: %s  queue:%s%n",
                                  clusterName, queueName);
        str += String.format("  predicted start time: %s +- %s%n",
                             df.format(predictedStartTime),Util.hms(confidenceIntervalSecs));
        str += String.format("            (wait time: %20s +- %s)%n",
                             Util.hms(predictedWaitTimeSecs),Util.hms(confidenceIntervalSecs));
        return str;

}
};


public static void main(String[] args) {
        if (args.length < 3) {
                System.err.println("usage: QsPrediction <queue@cluster> ..<queue@cluster2> <processors> <run time (hh:mm:ss)>");
                System.exit(1);
        }

        Karnak k = new Karnak();
        try {
                // last argument is HH:MM:SS formatted string
                String hms[] = args[args.length-1].split(":");
                if ( hms.length == 3) {
                        int runTimeSecs = Integer.parseInt(hms[2])+60*(Integer.parseInt(hms[1])+60*Integer.parseInt(hms[0]));

                        List<String> queries = new ArrayList<String>();
                        for(int index=0; index < args.length-2; index++)
                                queries.add(args[index]);

                        System.out.print(k.getMultiplePredictions(Integer.parseInt(args[args.length-2]),runTimeSecs, queries));
                }else{
                        System.err.println(args[args.length-1]+" is bad formatted. Aborting");
                }

        } catch (KarnakException e) {
                e.printStackTrace();
        }
}

}
