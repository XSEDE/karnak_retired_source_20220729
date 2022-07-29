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

import java.util.*;
import java.text.SimpleDateFormat;
import javax.ws.rs.*;
import javax.ws.rs.core.*;

import karnak.service.*;

@Path("/karnak/starttime")
public class StartTimeResource extends TimeResource {
    
    @Path("index.txt")
    @GET 
    @Produces("text/plain")
    public String getSystemsText() {
	return super.getSystemsText();
    }
    
    @Path("index.html")
    @GET 
    @Produces("text/html")
    public String getSystemsHtml() {
	return super.getSystemsHtml();
    }
    
    @Path("index.xml")
    @GET 
    @Produces("text/xml")
    public String getSystemsXml() {
	return super.getSystemsXml();
    }
    
    @Path("index.json")
    @GET 
    @Produces("application/json")
    public String getSystemsJson() {
	return super.getSystemsJson();
    }

    @Path("predict_form.html")
    @GET 
    @Produces("text/html")
    public String getPredictFormHtml() {
	return getPredictFormHtml(true);
    }

    @Path("predict.html")
    @GET 
    @Produces("text/html")
    public String getPredictHtml(@QueryParam("system") List<String> systems,
				 @QueryParam("queue") List<String> queueAtSystems,
				 @DefaultValue("1") @QueryParam("cores") String coresStr,
				 @DefaultValue("1") @QueryParam("hours") String hours,
				 @DefaultValue("0") @QueryParam("minutes") String minutes,
				 @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return super.getPredictHtml(false,systems,queueAtSystems,coresStr,hours,minutes,confidenceStr);
    }

    @Path("prediction/")
    @POST
    @Consumes("text/plain")
    @Produces("text/plain")
    public String postPredictText(String content,
				  @Context UriInfo uriInfo) {
	String id = super.predictText(content);
	UriBuilder ub = uriInfo.getAbsolutePathBuilder();
	return ub.path(id+".txt").build().getPath();
    }

    @Path("prediction/")
    @POST
    @Consumes("text/xml")
    @Produces("text/xml")
    public String postPredictXml(String content,
				 @Context UriInfo uriInfo) {
	String id = super.predictXml(content);
	UriBuilder ub = uriInfo.getAbsolutePathBuilder();
	return ub.path(id+".xml").build().getPath();
    }

    @Path("prediction/")
    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public String postPredictJson(String content,
				  @Context UriInfo uriInfo) {
	String id = super.predictJson(content);
	UriBuilder ub = uriInfo.getAbsolutePathBuilder();
	return ub.path(id+".txt").build().getPath();
    }

    @Path("prediction/{id}.txt")
    @GET 
    @Produces("text/plain")
    public String getPredictionText(@PathParam("id") String id) {
	return super.getPredictionText(false,id);
    }

    @Path("prediction/{id}.xml")
    @GET 
    @Produces("text/xml")
    public String getPredictionXml(@PathParam("id") String id) {
	return super.getPredictionXml(false,id);
    }

    @Path("prediction/{id}.json")
    @GET 
    @Produces("application/json")
    public String getPredictionJson(@PathParam("id") String id) {
	return super.getPredictionJson(false,id);
    }

    
    @Path("system/{system}/job/waiting.txt")
    @GET 
    @Produces("text/plain")
    public String getSystemText(@PathParam("system") String system) {
	system = system.replace("teragrid.org","xsede.org");
	return JobResource.getWaitingJobsText(system);
    }
    
    @Path("system/{system}/job/waiting.html")
    @GET 
    @Produces("text/html")
    public String getSystemHtml(@PathParam("system") String system,
	                        @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	system = system.replace("teragrid.org","xsede.org");
	return JobResource.getWaitingJobsHtml(system,false,true);
    }
    
    @Path("system/{system}/job/waiting.xml")
    @GET 
    @Produces("text/xml")
    public String getSystemXml(@PathParam("system") String system) {
	system = system.replace("teragrid.org","xsede.org");
	return JobResource.getWaitingJobsXml(system);
    }
    
    @Path("system/{system}/job/waiting.json")
    @GET 
    @Produces("application/json")
    public String getSystemJson(@PathParam("system") String system) {
	system = system.replace("teragrid.org","xsede.org");
	return JobResource.getWaitingJobsJson(system);
    }


    /*
    @Path("{system}/{jobId}/prediction.txt")
    @GET 
    @Produces("text/plain")
    public String getJobText(@PathParam("system") String system,
			     @PathParam("jobId") String jobId,
			     @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getJobText(false,system,jobId,confidenceStr);
    }

    @Path("{system}/{jobId}/prediction.html")
    @GET 
    @Produces("text/html")
    public String getJobHtml(@PathParam("system") String system,
			     @PathParam("jobId") String jobId,
			     @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getJobHtml(false,system,jobId,confidenceStr);
    }

    @Path("{system}/{jobId}/prediction.xml")
    @GET 
    @Produces("text/xml")
    public String getJobXml(@PathParam("system") String system,
			    @PathParam("jobId") String jobId,
			    @DefaultValue("90") @QueryParam("confidence") String confidenceStr) {
	return getJobXml(false,system,jobId,confidenceStr);
    }
    */
}
