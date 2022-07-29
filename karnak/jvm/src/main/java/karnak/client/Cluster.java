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

public class Cluster {
    public String name = null;
    public JobSummary summary = null;
    public List<JobStatistics> stats = new Vector<JobStatistics>();

    public List<Queue> queues = new Vector<Queue>();

    public static Cluster fromDom(Element element) {
	Cluster sys = new Cluster(element.getElementsByTagName("Name").item(0).getTextContent());
	sys.summary = JobSummary.fromDom(element);
	return sys;
    }

    public Cluster(String name) {
	this.name = name;
    }

    public String toString() {
	String endl = System.getProperty("line.separator");
	String str = "System "+name+endl;
	if (summary != null) {
	    str += summary.toString().replaceAll("(?m)^", "  ");
	}
	for(Queue queue: queues) {
	    str += queue.toString().replaceAll("(?m)^", "  ");
	}

	return str;
    }


    public static void main(String[] args) {
	if (args.length > 1) {
	    System.err.println("usage: Cluster [cluster]");
	    System.exit(1);
	}

	Karnak k = new Karnak();
	try {
	    if (args.length == 0) {
		for(Cluster cluster: k.clusters()) {
		    System.out.print(cluster);
		}
	    } else {
		System.out.print(k.cluster(args[0]));
	    }
	} catch (KarnakException e) {
	    e.printStackTrace();
	}
    }

}
