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

package karnak.service.predict;
import java.util.Date;


public class SubmittedStartTimeQuery {
    public String system = null;
    public String jobId = null;
    public int intervalPercent = 90;
    /* QueryTime is exclusively used when the backtest is done
      During backtest, specific query time must be given to the predictor
      so that valid decision tree at that point can be constructed
      */
    public Date querytime;    

    public SubmittedStartTimeQuery() {
        querytime = new Date();
    }

    public SubmittedStartTimeQuery(Date given) {
        querytime = given;
    }


    @Override
    public String toString() {

        StringBuilder strbuilder = new StringBuilder();

        strbuilder.append("SubmittedStartTimeQuery {");
        strbuilder.append("\n");
        strbuilder.append("system=" + system);
        strbuilder.append("\n");
        strbuilder.append("jobId=" + jobId);
        strbuilder.append("\n");
        strbuilder.append("intervalPercent=" + intervalPercent);
        strbuilder.append("\n");
        strbuilder.append("}");
        strbuilder.append("\n");
        return strbuilder.toString();
    }

    public String getSystem() {
        return system;
    }

    public void setSystem(String system) {
        this.system = system;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public int getIntervalPercent() {
        return intervalPercent;
    }

    public void setIntervalPercent(int intervalPercent) {
        this.intervalPercent = intervalPercent;
    }

    
}
