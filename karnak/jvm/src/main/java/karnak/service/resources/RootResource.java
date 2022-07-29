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

package karnak.service.resources;

import java.io.File;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("/karnak")
public class RootResource {
    
    @Path("index.html")
    @GET 
    @Produces("text/html")
    public String getHtml() {
	String text = "<title>Karnak Prediction Service</title>\n\n";
	text = text + "<h1>Karnak Prediction Service</h1>\n\n";
	text = text + "<p>This service provides predictions and information about jobs on batch scheduled "+
	    "computer systems.</p>\n\n";
	text = text + "<ul>\n";
	text = text + "<li> <a href='system/status.html'>System Information</a>\n";
	text = text + "<li> <a href='waittime/index.html'>Wait Time Predictions</a>\n";
	text = text + "<li> <a href='starttime/index.html'>Start Time Predictions</a>\n";
	text = text + "</ul>\n";

	text = text + "<p>If you wish to access the Karnak service via the command line, ";
	text = text + "<a href='karnak-client-1.0b1.tar.gz'>client programs</a> are available ";
	text = text + "(Python interpreter required).\n";

	text = text + "<p>If you wish to access the Karnak service using Java, ";
	text = text + "<a href='karnak-client-1.0a3.jar'>a Java client library</a> is available ";
	text = text + "(Requires the <a href='http://hc.apache.org/downloads.cgi'>Apache http client</a>).\n";

	return text;
    }

    @Path("karnak-client-1.0b1.tar.gz")
    @GET 
    @Produces("application/octet-stream")
    public File getClient() {
	return new File("/home/karnak/karnak/var/www/karnak-1.0b1.tar.gz");
    }

    @Path("karnak-client-1.0a3.jar")
    @GET 
    @Produces("application/octet-stream")
    public File getJavaClient() {
	return new File("/home/karnak/karnak/var/www/karnak-client-1.0a3.jar");
    }
}
