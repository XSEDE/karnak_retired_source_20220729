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

package karnak.client.v2;


import java.io.*;
import java.util.*;


import javax.xml.parsers.*;

import org.w3c.dom.*;

// need Apache http client from http://hc.apache.org/downloads.cgi
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
/* Jungha Woo
 * Deprecated
   import org.apache.http.impl.client.DefaultHttpClient;
 */
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
class Karnak {

public static final String DEFAULT_HOSTNAME = "karnak.xsede.org";
private final String host;
private final String unsubmitted_prediction_uri;
private final String submitted_prediction_uri;
private final String systems_uri;


public Karnak() {
        this(Karnak.DEFAULT_HOSTNAME);
}

public Karnak(String host) {
        this.host = host;
        unsubmitted_prediction_uri = "http://"+host+"/karnak/v2/starttime/prediction/";
        /* Jobs already in the queue but have not started yet */
        submitted_prediction_uri = "http://"+host+"/karnak/v2/starttime/system/";
        systems_uri = "http://"+host+"/karnak/system/status.xml";
}


public List<Cluster> clusters() throws KarnakException {
        Document doc = get(systems_uri);
        //printDocument(doc);

        Vector<Cluster> clusters = new Vector<Cluster>();

        NodeList clusterNodes = doc.getElementsByTagName("System");
        for(int i=0; i<clusterNodes.getLength(); i++) {
                clusters.add(Cluster.fromDom((Element)clusterNodes.item(i)));
        }

        return clusters;
}

public Cluster cluster(String name) throws KarnakException {
        Document doc = get("http://"+host+"/karnak/system/"+name+"/status.xml");
        //printDocument(doc);

        try {
                throw new KarnakException(doc.getElementsByTagName("Error").item(0).getTextContent());
        } catch (NullPointerException e) {}

        Cluster cluster = Cluster.fromDom(doc.getDocumentElement());

        NodeList queueNodes = doc.getElementsByTagName("Queue");
        for(int i=0; i<queueNodes.getLength(); i++) {
                Queue queue = Queue.fromDom((Element)queueNodes.item(i));
                if (queue.name.equals("all_jobs")) {
                        cluster.summary = queue.summary;
                } else {
                        cluster.queues.add(queue);
                }
        }

        return cluster;
}

public Queue queue(String clusterName, String queueName) throws KarnakException {
        Document doc = get("http://"+host+"/karnak/system/"+clusterName+"/queue/"+queueName+"/summary.xml");
        //printDocument(doc);

        try {
                throw new KarnakException(doc.getElementsByTagName("Error").item(0).getTextContent());
        } catch (NullPointerException e) {}

        Cluster cluster = Cluster.fromDom(doc.getDocumentElement());
        Queue queue = Queue.fromDom((Element)doc.getElementsByTagName("Queue").item(0));
        queue.clusterName = cluster.name;

        return queue;
}

public List<Job> waitingJobs(String clusterName) throws KarnakException {
        Document doc = get("http://"+host+"/karnak/system/"+clusterName+"/job/waiting.xml");
        //printDocument(doc);

        Vector<Job> jobs = new Vector<Job>();

        NodeList nodes = doc.getElementsByTagName("Job");
        for(int i=0; i<nodes.getLength(); i++) {
                Job job = Job.fromDom((Element)nodes.item(i));
                job.clusterName = clusterName;
                jobs.add(job);
        }

        return jobs;
}

public QueuedPrediction queuedPrediction(String clusterName, String jobIdentifier) throws KarnakException {

        /*
           Corresponding Python URI
           conn.request("GET","/karnak/v2/starttime/system/"+args[0]+"/job/"+args[1]+"/prediction/starttime."+format,"",headers)
         */
        // this could also be to waittime.xml - it doesn't matter
        Document doc = get(submitted_prediction_uri+clusterName+"/job/"+jobIdentifier+
                           "/prediction/starttime.xml");
        //Util.printDocument(doc);

        try {
                throw new KarnakException(doc.getElementsByTagName("Error").item(0).getTextContent());
        } catch (NullPointerException e) {}

        return QueuedPrediction.fromDom(doc);
}

public QueuePrediction queuePrediction(int processors, int requestedRunTimeSecs,
                                       String clusterName, String queueName) throws KarnakException {

        String body = String.format("<Predictions xmlns='http://tacc.utexas.edu/karnak/protocol/1.0'>%n");
        body += String.format("  <Processors>%d</Processors>%n",processors);
        body += String.format("  <RequestedWallTime units='minutes'>%d</RequestedWallTime>%n",
                              requestedRunTimeSecs/60);
        body += String.format("  <Confidence>%d</Confidence>%n",90);
        body += String.format("  <Location system='%s' queue='%s'/>%n",clusterName,queueName);
        body += String.format("  <Format> Raw </Format>%n");
        body += String.format("  <NumGroups>1</NumGroups>%n");
        body += String.format("  <Labels></Labels>%n");
        body += String.format("</Predictions>%n");

        /*
           @Deprecated
           DefaultHttpClient httpClient = new DefaultHttpClient();
         */

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        // /waittime/prediction/ would also work
        HttpPost hpost = new HttpPost(unsubmitted_prediction_uri);
        hpost.addHeader("Content-type", "text/xml");
        hpost.addHeader("Accept", "text/xml");
        try {
                hpost.setEntity(new StringEntity(body));
        } catch (UnsupportedEncodingException e) {
                throw new KarnakException(e);
        }

        CloseableHttpResponse response = null;
        try {
                response = httpClient.execute(hpost);
        } catch (IOException e) {

                try {
                        response.close();
                        httpClient.close();

                } catch (IOException iex) {
                        //do something clever with the exception
                }

                throw new KarnakException(e);
        }
        if (response.getStatusLine().getStatusCode() != 200) {
                try {
                        response.close();
                        httpClient.close();

                } catch (IOException iex) {
                        //do something clever with the exception
                }

                throw new KarnakException("HTTP POST failed: " + response.getStatusLine().getStatusCode());
        }

        String path = null;
        try {
                Scanner s = new Scanner(response.getEntity().getContent()).useDelimiter("\\A");
                path = s.hasNext() ? s.next() : "";
        } catch (IOException e) {
                e.printStackTrace();
        } finally{

                if(response != null) {
                        try {
                                response.close();
                                httpClient.close();

                        } catch (IOException e) {
                                //do something clever with the exception
                        }
                }
        }


        /*
           @Deprecated
           httpClient.getConnectionManager().shutdown();
         */


        Document doc = get("http://"+host+path);
        //Util.printDocument(doc);

        try {
                throw new KarnakException(doc.getElementsByTagName("Error").item(0).getTextContent());
        } catch (NullPointerException e) {}

        return QueuePrediction.fromDom(doc);
}

/*
   List<String> queueAtSystemList is a list of "queue@system" formatted String.
   For example, "default@gordon.sdsc.xsede.org"
 */
public QsPrediction getMultiplePredictions(int processors, int requestedRunTimeSecs, List<String> queueAtSystemList
                                           ) throws KarnakException {

        String body = String.format("<Predictions xmlns='http://tacc.utexas.edu/karnak/protocol/1.0'>%n");
        body += String.format("  <Processors>%d</Processors>%n",processors);
        body += String.format("  <RequestedWallTime units='minutes'>%d</RequestedWallTime>%n",
                              requestedRunTimeSecs/60);
        body += String.format("  <Confidence>%d</Confidence>%n",90);

        for( String query : queueAtSystemList) {
                String[] items = query.split("@");
                if( items.length == 2) {
                        body += String.format("  <Location system='%s' queue='%s'/>%n", items[1], items[0] );
                }else{
                        System.out.println(query+" is not formatted as queue@system. Ignoring..");
                }
        }
        body += String.format("  <Format> Raw </Format>%n");
        body += String.format("  <NumGroups>1</NumGroups>%n");
        body += String.format("  <Labels></Labels>%n");
        body += String.format("</Predictions>%n");

        /*
           @Deprecated
           DefaultHttpClient httpClient = new DefaultHttpClient();
         */

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        // /waittime/prediction/ would also work
        HttpPost hpost = new HttpPost(unsubmitted_prediction_uri);
        hpost.addHeader("Content-type", "text/xml");
        hpost.addHeader("Accept", "text/xml");
        try {
                hpost.setEntity(new StringEntity(body));
        } catch (UnsupportedEncodingException e) {
                throw new KarnakException(e);
        }

        CloseableHttpResponse response = null;
        try {
                response = httpClient.execute(hpost);
        } catch (IOException e) {

                try {
                        response.close();
                        httpClient.close();

                } catch (IOException iex) {
                        //do something clever with the exception
                }

                throw new KarnakException(e);
        }
        if (response.getStatusLine().getStatusCode() != 200) {

                try {
                        response.close();
                        httpClient.close();

                } catch (IOException iex) {
                        //do something clever with the exception
                }

                throw new KarnakException("HTTP POST failed: " + response.getStatusLine().getStatusCode());
        }

        String path = null;
        try {
                Scanner s = new Scanner(response.getEntity().getContent()).useDelimiter("\\A");
                path = s.hasNext() ? s.next() : "";
        } catch (IOException e) {
                e.printStackTrace();
        } finally{

                if(response != null) {
                        try {
                                response.close();
                                httpClient.close();
                        } catch (IOException e) {
                                //do something clever with the exception
                        }
                }
        }


        /*
           @Deprecated
           httpClient.getConnectionManager().shutdown();
         */


        Document doc = get("http://"+host+path);
        //Util.printDocument(doc);

        try {
                throw new KarnakException(doc.getElementsByTagName("Error").item(0).getTextContent());
        } catch (NullPointerException e) {}

        return QsPrediction.fromDom(doc);
}



/*
   This function sends a query and received grouped prediction results.
   queue@systems having similar prediction results are classified into same group,
   but their ordering in a group is random so should not be interpreted as
   any order.

   To retrieve the results,

   List<String> queueAtSystemList is a list of "queue@system" formatted String.
   For example, "default@gordon.sdsc.xsede.org"
 */
public GroupPrediction getGrpPredictions(int processors, int requestedRunTimeSecs, List<String> queueAtSystemList, List<String> labels
                                         ) throws KarnakException {

        String body = String.format("<Predictions xmlns='http://tacc.utexas.edu/karnak/protocol/1.0'>%n");
        body += String.format("  <Processors>%d</Processors>%n",processors);
        body += String.format("  <RequestedWallTime units='minutes'>%d</RequestedWallTime>%n",
                              requestedRunTimeSecs/60);
        body += String.format("  <Confidence>%d</Confidence>%n",90);

        for( String query : queueAtSystemList) {
                String[] items = query.split("@");
                if( items.length == 2) {
                        body += String.format("  <Location system='%s' queue='%s'/>%n", items[1], items[0] );
                }else{
                        System.out.println(query+" is not formatted as queue@system. Ignoring..");
                }
        }
        body += String.format("  <Format>LabeledGroup</Format>%n");

        if( labels == null || labels.size() == 0 ) {
                throw new KarnakException("The labels MUST be present for grouped prediction request.");
        }

        body += String.format("  <NumGroups>"+ labels.size()+"</NumGroups>%n");


        // create comman separated list
        StringBuilder sb = new StringBuilder();
        for (String s : labels)
        {
                sb.append(s);
                sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1); // remove last comma
        //System.out.println(sb.toString());

        body += String.format("<Labels>"+sb.toString()+"</Labels>%n");
        body += String.format("</Predictions>%n");

        /*
           @Deprecated
           DefaultHttpClient httpClient = new DefaultHttpClient();
         */

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        // /waittime/prediction/ would also work
        HttpPost hpost = new HttpPost(unsubmitted_prediction_uri);
        hpost.addHeader("Content-type", "text/xml");
        hpost.addHeader("Accept", "text/xml");
        try {
                hpost.setEntity(new StringEntity(body));
        } catch (UnsupportedEncodingException e) {
                throw new KarnakException(e);
        }

        CloseableHttpResponse response = null;
        try {
                response = httpClient.execute(hpost);
        } catch (IOException e) {

                try {
                        response.close();
                        httpClient.close();

                } catch (IOException iex) {
                        //do something clever with the exception
                }

                throw new KarnakException(e);
        }
        if (response.getStatusLine().getStatusCode() != 200) {

                try {
                        response.close();
                        httpClient.close();

                } catch (IOException iex) {
                        //do something clever with the exception
                }

                throw new KarnakException("HTTP POST failed: " + response.getStatusLine().getStatusCode());
        }

        String path = null;
        try {
                Scanner s = new Scanner(response.getEntity().getContent()).useDelimiter("\\A");
                path = s.hasNext() ? s.next() : "";
        } catch (IOException e) {
                e.printStackTrace();
        } finally{

                if(response != null) {
                        try {
                                response.close();
                                httpClient.close();
                        } catch (IOException e) {
                                //do something clever with the exception
                        }
                }
        }


        /*
           @Deprecated
           httpClient.getConnectionManager().shutdown();
         */


        Document doc = get("http://"+host+path);
        //Util.printDocument(doc);

        try {
                throw new KarnakException(doc.getElementsByTagName("Error").item(0).getTextContent());
        } catch (NullPointerException e) {}

        return GroupPrediction.fromDom(doc);
}



protected Document get(String url) throws KarnakException {
        /*
           @Deprecated
           DefaultHttpClient httpClient = new DefaultHttpClient();
         */

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();


        //System.out.println("getting "+url);
        HttpGet hget = new HttpGet(url);
        //hget.addHeader("accept", "application/xml");

        CloseableHttpResponse response = null;
        try {
                response = httpClient.execute(hget);
        } catch (IOException e) {
                if(response != null) {
                        try {
                                response.close();
                                httpClient.close();

                        } catch (IOException iex) {
                                //do something clever with the exception
                        }
                }

                throw new KarnakException(e);
        }
        if (response.getStatusLine().getStatusCode() != 200) {
                if(response != null) {
                        try {
                                response.close();
                                httpClient.close();

                        } catch (IOException iex) {
                                //do something clever with the exception
                        }
                }
                throw new KarnakException("HTTP request failed: " + response.getStatusLine().getStatusCode());
        }

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = null;
        try {
                db = dbf.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
                throw new KarnakException(e);
        }
        Document doc;
        try {
                doc = db.parse(response.getEntity().getContent());
        } catch (Exception e) {
                throw new KarnakException(e);
        } finally{

                if(response != null) {
                        try {
                                response.close();
                                httpClient.close();

                        } catch (IOException e) {
                                //do something clever with the exception
                        }
                }
        }

        if (doc.getDocumentElement().getTagName().equals("Error")) {
                throw new KarnakException(doc.getDocumentElement().getTextContent());
        }

        return doc;
}

}
