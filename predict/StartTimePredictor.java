
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
import karnak.service.Service;
import karnak.service.task.*;

public abstract class StartTimePredictor {

    private static Logger logger = Logger.getLogger(StartTimePredictor.class.getName());

    protected Service service = null;
    protected TreeMap<String,Predictor> predictors = new TreeMap<String,Predictor>();

    protected int maxInitialSize = 50000;

    public StartTimePredictor(Service service) {
	this.service = service;

	// this approach requires a restart to pick up new systems
	for(String system : service.getSystemNames()) {
	    service.taskThread.add(new ExperienceTask(system));
	}
    }


    class ExperienceTask extends PeriodicTask {
	protected String system = null;

	//protected long earliestTime = -1;
	protected long latestTime = -1;
	protected long totalExperiences = 0;

	public ExperienceTask(String system) {
	    super(15*60);
	    this.system = system;
	    configurePredictor(system);
	}

	public void runTask() {
	    logger.info("updating experiences for "+system);
	    if (totalExperiences == 0) {
		addInitialExperiences();
	    } else {
		addNewExperiences();
	    }
	}

	protected void addInitialExperiences() {
	    logger.info("loading initial wait time experiences for "+system+"...");
	    long curTime = Service.currentEpoch();
	    latestTime = curTime;
	    long interval = 14*24*60*60;
	    while ((totalExperiences < maxInitialSize) && (curTime > latestTime - 6*31*24*60*60)) {
		totalExperiences += addExperiences(system,curTime-interval,curTime);
		curTime -= interval;
	    }
	    logger.info("  loaded "+totalExperiences+" experiences");
	}

	protected void addNewExperiences() {
	    logger.info("loading new wait time experiences for "+system+"...");
	    long curTime = Service.currentEpoch();
	    int count = addExperiences(system,latestTime,curTime);
	    totalExperiences += count;
	    latestTime = curTime;
	    logger.info("  loaded "+count+" new experiences for a total of "+totalExperiences);
	}
    }


    protected abstract void configurePredictor(String system);

    protected abstract int addExperiences(String system, long earliestStartTime, long latestStartTime);


    protected Predictor getPredictor(String fileName, PredictionFactory predFactory) {
	logger.info("  predictor properties from "+fileName);
	java.util.Properties props = new Properties();
	try {
	    props.load(new FileInputStream(fileName));
	} catch (IOException e) {
	    logger.error("failed to load properties from file "+fileName);
	    System.exit(1);
	}
	return getPredictor(props,predFactory);
    }

    protected Predictor getPredictor(Properties props, PredictionFactory predFactory) {
	MultiPredictorFactory factory = new MultiPredictorFactory(predFactory);
	return factory.getPredictor(props);
    }

    //public abstract WaitTimePrediction predict(WaitTimeExperience query) throws PredictException;

    protected void logNearestNeighbors(WaitTimeExperience job, int k) {
	Predictor predictor = predictors.get(job.system);
	List<Predictor> predictors = ((MultiPredictor)predictor).getPredictors();
	// the first predictor better be the right one...
	ExperienceBase exb = ((IblPredictor)predictors.iterator().next()).getExperienceBase();
	Collection<Distance> dist = exb.nearestNeighbors(job,k);
	for(Distance d : dist) {
	    Feature swt = d.getExperience2().get("simulatedWaitTime");
	    Feature wt = d.getExperience2().get("waitTime");
	    if (swt == null) {
		logger.info("    sim wt (None) wt "+wt.getIntegerValue()+" distance "+d.getDistance());
	    } else {
		logger.info("    sim wt "+swt.getIntegerValue()+" wt "+wt.getIntegerValue()+
                            " distance "+d.getDistance());
	    }
	}
    }

}