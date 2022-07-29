
package karnak.service.predict;

import java.io.*;
import java.util.*;

import karnak.learn.*;

public class UnsubmittedWaitTimePredictionFactory implements PredictionFactory {

    public UnsubmittedWaitTimePredictionFactory() {
    }

    public ExperienceSchema getSchema() {
	return UnsubmittedWaitTimeExperience.getWaitTimeSchema();
    }

    public Prediction getPrediction() {
	return new UnsubmittedWaitTimePrediction();
    }

    public Prediction getPrediction(Experience exp) {
        if (!(exp instanceof WaitTimeExperience)) {
            return null;
        }
	return new UnsubmittedWaitTimePrediction((UnsubmittedWaitTimeExperience)exp);
    }

    public Prediction getPrediction(Prediction pred) {
        if (!(pred instanceof WaitTimePrediction)) {
            return null;
        }
	return new UnsubmittedWaitTimePrediction((UnsubmittedWaitTimePrediction)pred);
    }

}

