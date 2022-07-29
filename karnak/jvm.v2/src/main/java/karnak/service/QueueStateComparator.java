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

import java.util.Comparator;

class QueueStateComparator implements Comparator<QueueState> {

    public QueueStateComparator() {
    }

    public int compare(QueueState qs1, QueueState qs2) {
	if (qs1.time.before(qs2.time)) {
	    return -1;
	} else if (qs1.time.after(qs2.time)) {
	    return 1;
	} else {
	    return 0;
	}
    }

}
