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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class QueueState {
    public String system = null;
    public Date time = null;
    public List<String> jobIds = new ArrayList<String>();
    public Map<String,Job> jobs = new TreeMap<String,Job>();

    public QueueState() {
    }

    public QueueState(ResultSet rs) throws SQLException {
	system = rs.getString("system");

	// database contains times in UTC, but JDBC is assuming they are in the local time zone
	//time = rs.getTimestamp("time");

	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S z");
	try {
	    time = format.parse(rs.getString("time")+" UTC");
	} catch (ParseException e) {
	    throw new SQLException(e.getMessage());
	}
	
	StringTokenizer tok = new StringTokenizer(rs.getString("jobIds"));
	while(tok.hasMoreTokens()) {
	    jobIds.add(tok.nextToken());
	}
    }

}
