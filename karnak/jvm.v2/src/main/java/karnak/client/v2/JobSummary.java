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

package karnak.client.v2;


import org.w3c.dom.*;

public class JobSummary {
public int numRunningJobs = -1;
public int numWaitingJobs = -1;
public int usedProcessors = -1;

public static JobSummary fromDom(Element element) {
        JobSummary summary = new JobSummary();
        try {
                summary.numRunningJobs = Util.getChildContentInt(element,"NumRunningJobs");
        } catch (NullPointerException e) {}
        try {
                summary.numWaitingJobs = Util.getChildContentInt(element,"NumWaitingJobs");
        } catch (NullPointerException e) {}
        try {
                summary.usedProcessors = Util.getChildContentInt(element,"UsedProcessors");
        } catch (NullPointerException e) {}

        if ((summary.numRunningJobs == -1) && (summary.numWaitingJobs == -1) && (summary.usedProcessors == -1)) {
                return null;
        } else {
                return summary;
        }
}

public JobSummary() {
}

public String toString() {
        String endl = System.getProperty("line.separator");
        String str = "";
        if (numRunningJobs != -1) {
                str += "running jobs: "+numRunningJobs+endl;
        }
        if (numWaitingJobs != -1) {
                str += "waiting jobs: "+numWaitingJobs+endl;
        }
        if (usedProcessors != -1) {
                str += "used processors: "+usedProcessors+endl;
        }
        return str;
}
}
