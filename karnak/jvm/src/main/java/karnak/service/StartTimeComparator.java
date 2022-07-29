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

import java.util.*;

class StartTimeComparator implements Comparator<Job> {

    public StartTimeComparator() {
    }

    public int compare(Job job1, Job job2) {
	if (job1.startTime == null) {
	    if (job2.startTime == null) {
		return job1.hashCode() - job2.hashCode();
	    } else  {
		return 1;
	    }
	} else {
	    if (job2.startTime == null) {
		return -1;
	    } else {
		if (job1.startTime.before(job2.startTime)) {
		    return -1;
		} else if (job1.startTime.after(job2.startTime)) {
		    return 1;
		} else {
		    return job1.hashCode() - job2.hashCode();
		}
	    }
	}
    }

}
