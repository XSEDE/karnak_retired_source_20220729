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

import java.io.IOException;
import java.net.URI;
import java.text.*;
import java.util.*;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator; // for the hack

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import karnak.KarnakException;
import karnak.service.predict.*;

public class WebService implements Daemon {
    private static Logger logger = Logger.getLogger(WebService.class.getName());

    HttpServer server = null;

    private JobStatisticsCache jobStatsCache = new JobStatisticsCache();

    public static int MAX_AGE_SECS = 15 * 60;

    static {
	// hack for now because of sbt difficulties
	PropertyConfigurator.configure("/home/karnak/karnak/etc/log4j.properties");
    }

    public WebService() {
    }

    public static String dateToString(Date date) {
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));
	return df.format(date);
    }

    public static String dateToLocalString(Date date) {
	//DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	DateFormat df = new SimpleDateFormat("MM/dd HH:mm:ss");
	df.setTimeZone(getLocalTimeZone());
	return df.format(date);
    }

    public static String dateToSqlString(Date date) {
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	df.setTimeZone(TimeZone.getTimeZone("GMT"));
	return df.format(date);
    }

    public static TimeZone getLocalTimeZone() {
	return TimeZone.getTimeZone("America/Chicago");
    }

    public static String getLocalTimeZoneString() {
	SimpleDateFormat df = new SimpleDateFormat();

	df.setTimeZone(getLocalTimeZone());
	df.applyPattern("z");
	return df.format(new java.util.Date());
    }

    public static String hms(int seconds) {
	int hours = (int)Math.floor(seconds / (60*60));
	seconds = seconds - hours * (60*60);
	int minutes = (int)Math.floor(seconds / 60);
	seconds = seconds - minutes * 60;
	return String.format("%02d:%02d:%02d",hours,minutes,seconds);
    }

    public static final String BASE_URI = "http://0.0.0.0:8080/";

    public static HttpServer startServer() {
        final ResourceConfig rc = new ResourceConfig().packages("karnak.service.resources");
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    // daemon

    public void init(DaemonContext context) throws DaemonInitException, Exception {
    }

    public void start() throws Exception {
        server = startServer();
        logger.info("Jersey app started with WADL available at %sapplication.wadl".format(BASE_URI));
    }

    public void stop() throws Exception {
        server.stop();
	jobStatsCache.stopRunning();
    }

    public void destroy() {
    }

    public static void main(String[] args) throws IOException {

	Class c = WebService.class;
	System.out.println("methods:");
	java.lang.reflect.Method[] methods = c.getMethods();
	for(int i=0;i<methods.length;i++) {
	    System.out.println(methods[i]);
	}

	WebService service = new WebService();
	try {
	    service.start();
	    System.out.println("Jersey app started with WADL available at %sapplication.wadl".format(BASE_URI));
	    System.in.read();
	    System.out.println("Hit enter to stop...");
	    service.stop();
	} catch (Exception e) {
	    logger.error("failed to run service: "+e.getMessage());
	}
    }
}
