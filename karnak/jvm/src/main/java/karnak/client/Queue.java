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

package karnak.client;

import java.util.*;

import org.w3c.dom.*;

import karnak.KarnakException;

public class Queue {
    public String name = null;
    public String clusterName = null;
    public JobSummary summary = null;
    public List<JobStatistics> stats = new Vector<JobStatistics>();

    public static Queue fromDom(Element element) {
	Queue queue = new Queue(element.getElementsByTagName("Name").item(0).getTextContent());
	queue.summary = JobSummary.fromDom(element);
	NodeList nodes = element.getElementsByTagName("JobStatistics");
	for(int i=0;i<nodes.getLength();i++) {
	    JobStatistics s = JobStatistics.fromDom((Element)nodes.item(i));
	    queue.stats.add(s);
	}
	return queue;
    }

    public Queue(String name) {
	this.name = name;
    }

    public String toString() {
	String endl = System.getProperty("line.separator");
	String str = String.format("Queue %s on cluster %s%n",name,clusterName);
	if (summary != null) {
	    str += summary.toString().replaceAll("(?m)^", "  ");
	}
	for(JobStatistics s: stats) {
	    str += s.toString().replaceAll("(?m)^", "  ");
	}
	return str;
    }

    public static void main(String[] args) {
	if (args.length > 2) {
	    System.err.println("usage: Queue <cluster> [queue]");
	    System.exit(1);
	}

	String clusterName = args[0];
	String queueName = "all_jobs";
	if (args.length == 2) {
	    queueName = args[1];
	}

	Karnak k = new Karnak();
	try {
	    System.out.print(k.queue(clusterName,queueName));
	} catch (KarnakException e) {
	    e.printStackTrace();
	}
    }
}
