
package karnak.service.predict;

import java.io.*;
import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

import karnak.esim.*;
import karnak.learn.*;
//testing
import karnak.learn.ibl.*;
import karnak.learn.ibl.distance.*;
import karnak.learn.ibl.exb.*;

import karnak.predict.run.*;
import karnak.schedule.*;
import karnak.schedule.resource.*;

import karnak.service.*;
import karnak.service.task.*;

public class SubmittedStartTimePredictor extends StartTimePredictor {

    private static Logger logger = Logger.getLogger(SubmittedStartTimePredictor.class.getName());

    public SubmittedStartTimePredictor(Service service) {
	super(service);
    }

    protected void configurePredictor(String system) {
	if (System.getenv("KARNAK_HOME") == null) {
	    logger.error("cannot create wait time predictor - KARNAK_HOME environment variable not set");
	    System.err.println("cannot create wait time predictor - KARNAK_HOME environment variable not set");
	    System.exit(1);
	}

	String prefix = System.getenv("KARNAK_HOME")+"/etc/waittime_submitted_";

	String fileName = prefix+system+".props";
	File file = new File(fileName);
	if (!file.exists()) {
	    fileName = prefix+"default.props";
	}

	predictors.put(system,getPredictor(fileName,new SubmittedWaitTimePredictionFactory()));
    }

    protected int addExperiences(String system, long start, long end) {
	List<SubmittedWaitTimeExperience> experiences = getExperiences(system,start,end);
	if (experiences.size() > maxInitialSize) {
	    logger.warn("  only adding last "+maxInitialSize+" experiences of "+experiences.size()+
			" for "+experiences.get(0).system);
	    experiences = experiences.subList(experiences.size()-maxInitialSize,experiences.size());
	}
	addExperiences(experiences);
	return experiences.size();
    }

    protected List<SubmittedWaitTimeExperience> getExperiences(String system, long start, long end) {
	List<SubmittedWaitTimeExperience> experiences = new ArrayList<SubmittedWaitTimeExperience>();
	logger.debug("  checking for submitted wait time experiences for system "+system+" between "+
		     start+" and "+end+"...");

	try {
	    Connection conn = service.getKarnakConnection();
	    Statement stat = conn.createStatement();
	    ResultSet rs = stat.executeQuery("select * from submitted_experiences where system='"+system+"'"+
					     " and startTime>"+start+" and startTime<="+end+
					     " order by startTime asc;");
	    while (rs.next()) {
		SubmittedWaitTimeExperience exp = new SubmittedWaitTimeExperience(rs);
		// these should all have a start time, right?
		if (exp.startTime == -1) {
		    continue;
		}
		experiences.add(exp);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (Exception e) {
	    logger.error(e.getMessage());
	    e.printStackTrace();
	}
	return experiences;
    }

    protected void addExperiences(List<SubmittedWaitTimeExperience> experiences) {
	if (experiences.size() == 0) {
	    return;
	}
	logger.info("  adding "+experiences.size()+" submitted experiences for "+experiences.get(0).system);

	Predictor predictor = predictors.get(experiences.get(0).system);

	for(SubmittedWaitTimeExperience exp : experiences) {
	    try {
		predictor.insert(exp);
	    } catch (LearningException e) {
		logger.error("failed to insert wait time experience: "+e.getMessage());
		logger.debug(exp);
	    }
	}
    }

    public SubmittedWaitTimePrediction predict(SubmittedWaitTimeExperience query) throws PredictException {
	return predict(query,90);
    }

    public SubmittedWaitTimePrediction predict(SubmittedWaitTimeExperience query,
					       int confidence) throws PredictException {
	logger.info("predicting job "+query.id+" on "+query.system);
	if (!predictors.containsKey(query.system)) {
	    throw new PredictException("no predictor known for system "+query.system);
	}
	if (!service.getQueueNames(query.system).contains(query.queue)) {
	    throw new PredictException("queue "+query.queue+" is unknown on system "+query.system);
	}

	QueueState queue = service.getCurrentQueueState(query.system);
	if (queue == null) {
	    throw new PredictException("current queue state unknown");
	}
	query.setContext(queue);
	logger.info(query);

	Predictor predictor = predictors.get(query.system);
	try {
	    return (SubmittedWaitTimePrediction)predictor.predict(query,confidence);
	} catch (SchemaException e) {
	    throw new PredictException(e.getMessage());
	} catch (PredictException e2) {
	    //e2.printStackTrace();
	    throw e2;
	}
    }

    protected QueueState readQueueState(String system, long time) {
	logger.debug("  readQueueState for "+system+" at "+Service.epochToString(time));

	QueueState queue = null;
	try {
	    Connection conn = service.getGlueConnection();
	    Statement stat = conn.createStatement();
	    ResultSet rs = stat.executeQuery("select * from queue_states where system='"+system+"'"+" and time="+time);
	    if (rs.first()) {
		queue = new QueueState(rs);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (Exception e) {
	    logger.error("readQueueState failed: "+e.getMessage());
	}
	if (queue == null) {
	    return queue;
	}
	service.readJobs(queue);
	return queue;
    }
}