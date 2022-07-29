package karnak.service.response;

import karnak.KarnakException;

import karnak.service.predict.DummyRuntimePredictor;
import karnak.service.predict.RuntimePrediction;
import karnak.service.predict.Predictable;
import karnak.service.predict.UnsubmittedStartTimePrediction;
import karnak.service.predict.SubmittedStartTimePrediction;
import karnak.service.predict.UnsubmittedStartTimeQuery;
import karnak.service.predict.UnsubmittedPredictor;
import karnak.service.util.InputValidator;
import scala.Enumeration.Value;
import karnak.service.predict.WeightingEnum;

import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import karnak.service.util.SystemInfoQuery;
import karnak.service.WebService;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author wooj
 *
 * This class generates estimations for the start time and runtime of a job. The
 * prediction is made when the constructor is invoked.
 *
 * Estimated runtime is an attribute of UnsubmittedPredictionResponse. If this
 * class maintains two lists separately, one for start time, and the other for
 * runtime, sorting by one criteria makes the object state incorrect. For
 * example, sorting by estimated start time might not care about the runtime
 * order. Introducing a new object that holds start time estimation and runtime
 * estimation makes class design unnecessarily complicated. To make the
 * prediction response simple, the runtime is chosen to be an attribute of
 * prediction responses.
 *
 * Note that the FinishTimeQuery and FinishTimePrediction are deprecated and
 * their use are strongly discouraged.
 */
public class UnsubmittedPredictionResponse extends AbstractResponse {

    private static Logger logger = Logger.getLogger(UnsubmittedPredictionResponse.class.getName());

    private UnsubmittedPredictionResponse() {

    }

    public UnsubmittedPredictionResponse(List<UnsubmittedStartTimeQuery> queries) {

        preds = new ArrayList<Predictable>();

        for (UnsubmittedStartTimeQuery query : queries) {

            try {

                UnsubmittedStartTimePrediction prediction = new UnsubmittedStartTimePrediction(query);

                /* prediciton is not yet made.
                 Before make a prediction, check all the parameters are valid.
                 */
                if (InputValidator.validate(prediction) == false) {
                    prediction.setRunTime(0); //in seconds
                    preds.add(prediction);
                    continue;
                }

                /* Little bit tricky.
                 If no error in parameter found, we throw out previously
                 created prediction object and do actual prediction.
                 New prediction object will be created by predictor and
                 returned.
                 */
                prediction = UnsubmittedPredictor.getPrediction(query, WeightingEnum.Single);

                /*
                 Estimate runtime of a job based on the parameters to a 
                 specific application.
                 DummyRuntimePredictor simply returns the requested wall time
                 to the client as the estimated runtime of the submitted/unsubmitted jobs.
                
                 This DummyRuntimePredictor is a placeholder for RuntimePredictor
                 that will be written in Scala in the future.
                  
                 */
                /*  Note that we provide the UnsubmittedStartTimeQuery object query. 
                 DummyRuntimePredictor does not set up the intervalSecs internally.
                 Basically it assumes that the estimated runtime is constant so
                 confidence level is 100%. Therefore no confidence level exists.
                
                 This will be changed fo real RuntimePrediction.
                 RuntimePrediction will have confidence interval for the same
                 confidence level as the start time estimation. 
                 We must assume that start time and runtime are independent
                 in that case.
                 */
                Map<String, String> paramsMap = new HashMap<>();
                DummyRuntimePredictor dummyPredictor = DummyRuntimePredictor.getInstance();

                RuntimePrediction runPred = dummyPredictor.getPrediction(query, paramsMap);
                prediction.setRunTime(runPred.getRuntime()); // in seconds

                preds.add(prediction);

            } catch (Exception ex) {
                ex.printStackTrace();
                logger.debug(ex.getMessage());
                continue;
            }

        }
        logger.debug("prediction size:" + preds.size());

    }

}
