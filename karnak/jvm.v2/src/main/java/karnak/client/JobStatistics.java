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

public class JobStatistics {
    public Date start = null;
    public Date end = null;
    public StartedJobStatistics started = null;
    public CompletedJobStatistics completed = null;

    public static JobStatistics fromDom(Element element) {
	JobStatistics stats = new JobStatistics();

	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));
	try {
	    stats.start = df.parse(element.getAttribute("start"));
	} catch (ParseException e) {}
	try {
	    stats.end = df.parse(element.getAttribute("end"));
	} catch (ParseException e) {}

	stats.started = StartedJobStatistics.fromDom((Element)element.getElementsByTagName("Started").item(0));
	stats.completed = CompletedJobStatistics.fromDom((Element)element.getElementsByTagName("Completed").item(0));
	return stats;
    }

    public JobStatistics() {
    }

    public String toString() {
	String endl = System.getProperty("line.separator");
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));

	String str = String.format("JobStatistics from %s to %s%n",df.format(start),df.format(end));
	if (started != null) {
	    str += String.format("  started jobs:%n");
	    str += started.toString().replaceAll("(?m)^", "    ");
	}
	if (completed != null) {
	    str += String.format("  completed jobs:%n");
	    str += completed.toString().replaceAll("(?m)^", "    ");
	}
	return str;
    }

}
