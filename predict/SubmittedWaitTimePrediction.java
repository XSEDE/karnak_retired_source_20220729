
package karnak.service.predict;

import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.service.*;

public class SubmittedWaitTimePrediction extends SubmittedWaitTimeExperience implements WaitTimePrediction {

    private static Logger logger = Logger.getLogger(SubmittedWaitTimePrediction.class.getName());

    public Confidence conf = null;

    public SubmittedWaitTimePrediction() {
    }

    public SubmittedWaitTimePrediction(SubmittedWaitTimeExperience exp) {
        super(exp);
    }

    public SubmittedWaitTimePrediction(SubmittedWaitTimePrediction pred) {
        super((WaitTimeExperience)pred);
        conf = pred.conf;
    }

    public SubmittedWaitTimePrediction(Job job) {
        super(job);
    }

    public long getTime() {
        return time;
    }

    // Prediction
    public void setPrediction(int index, Object value, Confidence conf) throws SchemaException {
        if (index != 0) {
            throw new SchemaException("WaitTimePrediction does not have an output at index "+index);
        }

        if (time == -1) {
            throw new SchemaException("time is not set");
        }

        try {
            startTime = time + ((Integer)value).intValue();
        } catch (ClassCastException e) {
            throw new SchemaException("value is a "+value.getClass().getName()+", not an Integer");
        }
        this.conf = conf;
    }

    // Prediction
    public void setPrediction(String name, Object value, Confidence conf) throws SchemaException {
        if (!name.equals("waitTime")) {
            throw new SchemaException("WaitTimePrediction does not have an output named "+name);
        }
        setPrediction(0,value,conf);
    }

    // Prediction
    public FeaturePrediction getPrediction(int index) {
        if (index != 0) {
            return null;
        }
        try {
            return new FeaturePrediction(schema.getOutput(0),new Integer((int)(startTime-time)),conf);
        } catch (SchemaException e) {
            logger.error("failed to get prediction at index "+index+": "+e.getMessage());
            return null;
        }
    }

    // Prediction
    public FeaturePrediction getPrediction(String name) {
        if (!name.equals("waitTime")) {
            return null;
        }
        return getPrediction(0);
    }

}
