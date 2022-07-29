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

public class UnsubmittedStartTimePrediction extends UnsubmittedStartTimeQuery implements Predictable<UnsubmittedStartTimePrediction> {
    //public Date submitTime = new Date();

    public Date startTime = null;
    public long intervalSecs = 0;
    /* estimated run time.
     int type can only designate up to 24 days.
     so I choose to use long type.
     */
    private long runTime = 0; // in seconds

    /* During processing, if it runs into any critical errors and cannot
     generate valid respnose object, it records error message and returns.
     If any error message is set, getHTML(), getXML(), and getJSON() return
     error message until it is cleared.
     */
    String errorMessage = null;

    public UnsubmittedStartTimePrediction(UnsubmittedStartTimeQuery query) {
        system = query.system;
        queue = query.queue;
        processors = query.processors;
        requestedWallTime = query.requestedWallTime;
        intervalPercent = query.intervalPercent;
    }

    /* Jungha Woo
     1/26/2016
     Multiple predictions need to be sorted by startTime in increasing order
     for grouping systems.
    
     startTime can be null even after prediction is completed. 
     That's when the requirements cannot be met so predictor gives up 
     making prediction. In that case, some error message are set to 
     prediction object. 
     Even in that case, that specific object must be
     comparable to other UnsubmittedStartTimePrediction object.
     otherwise predictions cannot be grouped.
     
     the key idea for comparing null with other is that whenever null object is
    seen, it is regarded as the longest prediction time. then null objects
    will be located at the end of a increasing ordered list.
     */
    public int compareTo(UnsubmittedStartTimePrediction other) {
        
        
        if( startTime == null){
            return 1;
        }else if( other.startTime == null){
            return -1;
        }
        
        return startTime.compareTo(other.startTime);
    }

    public Date getPredictedTime() {
        return startTime;
    }
    
    /* runtime is in unit of seconds so transforms it to milliseconds */
    public Date getFinishTime() {
        return new Date(startTime.getTime() +  runTime* 1000L);
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long runTime) {
        this.runTime = runTime;
    }

    public String getSystemName() {
        return system;
    }

    public String getQueueName() {
        return queue;
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
