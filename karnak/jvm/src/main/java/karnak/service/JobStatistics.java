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

public class JobStatistics {

    public String system = null;
    public String queue = null;
    public Date curTime = null;
    public int historySecs = -1;

    public int numStartedJobs = 0;
    public float sumProcessors = 0;
    public float sumRequestedRunTime = 0;
    public float sumWaitTime = 0;

    public int numCompletedJobs = 0;
    public float sumCompletedRequestedRunTime = 0;
    public float sumCompletedProcessors = 0;
    public float sumRunTime = 0;

    public JobStatistics() {
    }

    public JobStatistics(String system, String queue, Date curTime, int historySecs) {
	this.system = system;
	this.queue = queue;
	this.curTime = curTime;
	this.historySecs = historySecs;
    }

    public boolean containsTime(Date time) {
	if (time.getTime() > curTime.getTime()) {
	    return false;
	}
	if (time.getTime() < curTime.getTime() - historySecs*1000) {
	    return false;
	}
	return true;
    }

    public int getMeanProcessors() {
	if (numStartedJobs == 0) {
	    return 0;
	}
	return (int)(sumProcessors / numStartedJobs);
    }

    public int getMeanRequestedRunTime() {
	if (numStartedJobs == 0) {
	    return 0;
	}
	return (int)(sumRequestedRunTime / numStartedJobs);
    }

    public String getMeanRequestedRunTimeHMS() {
	return WebService.hms(getMeanRequestedRunTime());
    }

    public int getMeanWaitTime() {
	if (numStartedJobs == 0) {
	    return 0;
	}
	return (int)(sumWaitTime / numStartedJobs);
    }

    public String getMeanWaitTimeHMS() {
	return WebService.hms(getMeanWaitTime());
    }

    public int getMeanCompletedRequestedRunTime() {
	if (numCompletedJobs == 0) {
	    return 0;
	}
	return (int)(sumCompletedRequestedRunTime / numCompletedJobs);
    }

    public String getMeanCompletedRequestedRunTimeHMS() {
	return WebService.hms(getMeanCompletedRequestedRunTime());
    }

    public int getMeanCompletedProcessors() {
	if (numCompletedJobs == 0) {
	    return 0;
	}
	return (int)(sumCompletedProcessors / numCompletedJobs);
    }

    public int getMeanRunTime() {
	if (numCompletedJobs == 0) {
	    return 0;
	}
	return (int)(sumRunTime / numCompletedJobs);
    }

    public String getMeanRunTimeHMS() {
	return WebService.hms(getMeanRunTime());
    }
}
