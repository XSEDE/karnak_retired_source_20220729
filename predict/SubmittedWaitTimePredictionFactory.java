
package karnak.service.predict;

import java.io.*;
import java.util.*;

import karnak.learn.*;

public class SubmittedWaitTimePredictionFactory implements PredictionFactory {

    public SubmittedWaitTimePredictionFactory() {
    }

    public ExperienceSchema getSchema() {
	return SubmittedWaitTimeExperience.getWaitTimeSchema();
    }

    public Prediction getPrediction() {
	return new SubmittedWaitTimePrediction();
    }

    public Prediction getPrediction(Experience exp) {
        if (!(exp instanceof WaitTimeExperience)) {
            return null;
        }
	return new SubmittedWaitTimePrediction((SubmittedWaitTimeExperience)exp);
    }

    public Prediction getPrediction(Prediction pred) {
        if (!(pred instanceof WaitTimePrediction)) {
            return null;
        }
	return new SubmittedWaitTimePrediction((SubmittedWaitTimePrediction)pred);
    }

}

