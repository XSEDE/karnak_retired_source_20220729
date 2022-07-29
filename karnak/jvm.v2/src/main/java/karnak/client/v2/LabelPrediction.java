package karnak.client.v2;


import java.text.*;
import java.util.*;
import org.w3c.dom.*;

/*
   LabelPrediction class holds prediction results for multiple queue@system.
   It uses composition design pattern.
   The resulting predictions are stored in GroupPrediction.
   Please note that getGrpPredictions() in Karnak is used.

   Please refer to the following usage:
   "usage: LabelPrediction <queue@cluster> ..<queue@cluster2> <processors> <run time (hh:mm:ss)> <label1,label2,...>"

   Parameter requirements
   -queue and cluster must be concatenated by @.
   -requested run time should be formatted as HH:MM:SS. If not, the program fails.
   -<label1,lable2,,,,labelN> MUST be comman seperated text.


   Sample input:
   LabelPrediction debug@gordon.sdsc.xsede.org normal@gordon.sdsc.xsede.org normal@stampede.tacc.xsede.org  1 1:00:00 Fast,Slow


   GroupPrediction output:
   prediction for job generated at 2016-10-20T08:09:29Z
     processors: 1
     requested run time: 01:00:00
     submit time: 2016-10-20T08:09:29Z
   group:Fast	system:gordon.sdsc.xsede.org	queue:debug
   group:Fast	system:gordon.sdsc.xsede.org	queue:normal
   group:Slow	system:stampede.tacc.xsede.org	queue:normal


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
public class LabelPrediction {


public static void main(String[] args) {
        if (args.length < 4) {
                System.err.println("usage: LabelPrediction <queue@cluster> ..<queue@cluster2> <processors> <run time (hh:mm:ss)> <label1,label2,...>");
                System.err.println("Labels MUST be comma seperated string without blanks");
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

                        List<String> labels = new ArrayList<String>(Arrays.asList(args[args.length-1].split(",")));

                        //labels MUST NOT be zero
                        if (labels.size() == 0 ) {
                                System.err.println("Invalid labels were provided. Please check the number of labels. Aborting...");
                                System.exit(1);
                        }

                        System.out.print(k.getGrpPredictions(Integer.parseInt(args[args.length-3]),runTimeSecs, queries, labels));
                }else{
                        System.err.println(args[args.length-1]+" is bad formatted. Aborting");
                }

        } catch (KarnakException e) {
                e.printStackTrace();
        }
}

}
