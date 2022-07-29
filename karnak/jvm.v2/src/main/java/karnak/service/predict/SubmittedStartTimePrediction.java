/**
 * *************************************************************************
 */
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
/**
 * *************************************************************************
 */
package karnak.service.predict;

import java.util.Date;

/* 
 Although the class name sounds like predicting the start time only,
 it estimates both start time and run time. 
 Start time is the time the predictor estimated a given job will be started.
 runTime is the estimated duration of a job represented in seconds after it gets started. 
 Finish time can be computed as the sum of the start time and runtime.
 */
public class SubmittedStartTimePrediction extends SubmittedStartTimeQuery implements Predictable<SubmittedStartTimePrediction> {

    //public Date submitTime = null;
    public int processors = 0;
    public int requestedWallTime = 0; // seconds

    public Date startTime = null;
    public long intervalSecs = 0;
    // estimated run time 
    private int runTime = 0; // in seconds

    /* During processing, if it runs into any critical errors and cannot
     generate valid respnose object, it records error message and returns.
     If any error message is set, getHTML(), getXML(), and getJSON() return
     error message until it is cleared.
     */
    String errorMessage = null;

    public SubmittedStartTimePrediction(SubmittedStartTimeQuery query) {
        system = query.system;
        jobId = query.jobId;
        intervalPercent = query.intervalPercent;
    }

    /* Jungha Woo
     1/26/2016
     Multiple predictions need to be sorted by startTime in increasing order
     for grouping systems.
    
     See compareTo function of UnsubmittedStartTimePrediction.
     */
    public int compareTo(SubmittedStartTimePrediction other) {

        if (startTime == null) {
            return 1;
        } else if (other.startTime == null) {
            return -1;
        }

        return startTime.compareTo(other.startTime);
    }

    public Date getPredictedTime() {
        return startTime;
    }

    /* runtime is in unit of seconds so transforms it to milliseconds */
    public Date getFinishTime() {
        return new Date(startTime.getTime() + runTime * 1000L);
    }

    public int getRunTime() {
        return runTime;
    }

    public void setRunTime(int runTime) {
        this.runTime = runTime;
    }

    /* to be used to log predicted result to a file 
     @Override public String toString(){
        
     StringBuilder builder = new StringBuilder();
        
     builder.append(" SubmittedStartTimePrediction ");
     builder.append(builder)
     }
     */
    public String getSystemName() {
        return system;
    }

    /* Class that does not have jobId field must return error messsage
     or 
     */
    public String getJobId() {
        return jobId;
    }

    public int getNumProcessors() {
        return processors;
    }

    public int getRequestedWallTime() {
        return requestedWallTime;
    }

    public int getIntervalPercent() {
        return intervalPercent;
    }

    public long getIntervalSecs() {
        return intervalSecs;
    }
    /* if any error occurs while preparing response,
     set the errorMessage. 
     */

    public boolean hasError() {
        return (errorMessage != null && !errorMessage.isEmpty()) ? true : false;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String msg) {
        errorMessage = msg;
    }

    public void clearError() {
        errorMessage = null;
    }
}
