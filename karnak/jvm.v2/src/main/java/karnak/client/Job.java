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

import java.text.*;
import java.util.*;
import org.w3c.dom.*;

import karnak.KarnakException;

public class Job {
    public String id = null;
    public String clusterName = null;
    public Date submitTime = null;
    public int processors = -1;
    public int requestedRunTimeSecs = -1;


    public static Job fromDom(Element element) {
	Job job = new Job();
	job.id = element.getElementsByTagName("Identifier").item(0).getTextContent();

	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));
	try {
	    job.submitTime = df.parse(element.getElementsByTagName("SubmitTime").item(0).getTextContent());
	} catch (ParseException e) {}
	job.processors = Integer.parseInt(element.getElementsByTagName("Processors").item(0).getTextContent());
	Element reqRunTime = (Element)element.getElementsByTagName("RequestedWallTime").item(0);
	// assume units is minutes
	job.requestedRunTimeSecs = Math.round(Float.parseFloat(reqRunTime.getTextContent()) * 60);

	return job;
    }

    public Job() {
    }

    public String toString() {
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));

	String str = String.format("Job %s on %s%n",id,clusterName);
	str += String.format("  submit time: %s%n",df.format(submitTime));
	str += String.format("  processors: %d%n",processors);
	str += String.format("  requested run time: %s%n",Util.hms(requestedRunTimeSecs));

	return str;
    }

    public static void main(String[] args) {
	if (args.length != 1) {
	    System.err.println("usage: Job <cluster>");
	    System.exit(1);
	}

	Karnak k = new Karnak();
	try {
	    for(Job job: k.waitingJobs(args[0])) {
		System.out.print(job);
	    }
	} catch (KarnakException e) {
	    e.printStackTrace();
	}
    }

}
