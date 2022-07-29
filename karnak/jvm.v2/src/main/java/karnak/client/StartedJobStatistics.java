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

public class StartedJobStatistics {
    public int numJobs = -1;
    public int meanProcessors = -1;
    public int meanRequestedRunTimeSecs = -1;
    public int meanWaitTimeSecs = -1;

    public static StartedJobStatistics fromDom(Element element) {
	if (element == null) {
	    return null;
	}

	StartedJobStatistics stats = new StartedJobStatistics();
	try {
	    stats.numJobs = Util.getChildContentInt(element,"NumJobs");
	} catch (NullPointerException e) {}
	try {
	    stats.meanProcessors = Util.getChildContentInt(element,"MeanProcessors");
	} catch (NullPointerException e) {}
	try {
	    stats.meanRequestedRunTimeSecs = Util.getChildContentHms(element,"MeanRequestedRunTime");
	} catch (NullPointerException e) {}
	try {
	    stats.meanWaitTimeSecs = Util.getChildContentHms(element,"MeanWaitTime");
	} catch (NullPointerException e) {}
	return stats;
    }

    public StartedJobStatistics() {
    }

    public String toString() {
	String str = "";
	if (numJobs != -1) {
	    str += String.format("jobs: %d%n",numJobs);
	}
	if (meanProcessors != -1) {
	    str += String.format("mean processors: %d%n",meanProcessors);
	}
	if (meanRequestedRunTimeSecs != -1) {
	    str += String.format("mean requested run time: %s%n",Util.hms(meanRequestedRunTimeSecs));
	}
	if (meanWaitTimeSecs != -1) {
	    str += String.format("mean wait time: %s%n",Util.hms(meanWaitTimeSecs));
	}
	return str;
    }
}
