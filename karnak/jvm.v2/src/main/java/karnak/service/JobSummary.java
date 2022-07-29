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

package karnak.service;

import java.util.Date;

public class JobSummary {
    public String system = null;
    public String queue = null;

    public Date time = null;

    public int numRunning = -1;
    public int numWaiting = -1;
    public int usedProcessors = -1;

    public JobSummary() {
    }

    public JobSummary(String system) {
	this.system = system;
    }

    public JobSummary(String system, String queue) {
	this.system = system;
	this.queue = queue;
    }

    public void increment(JobSummary summary) {
	time = summary.time;
	if (numRunning == -1) {
	    numRunning = summary.numRunning;
	} else if (summary.numRunning != -1) {
	    numRunning += summary.numRunning;
	}
	if (numWaiting == -1) {
	    numWaiting = summary.numWaiting;
	} else if (summary.numWaiting != -1) {
	    numWaiting += summary.numWaiting;
	}
	if (usedProcessors == -1) {
	    usedProcessors = summary.usedProcessors;
	} else if (summary.usedProcessors != -1) {
	    usedProcessors += summary.usedProcessors;
	}
    }

    public void setState(int numRunning, int numWaiting, int usedProcessors) {
	this.numRunning = numRunning;
	this.numWaiting = numWaiting;
	this.usedProcessors = usedProcessors;
    }
}
