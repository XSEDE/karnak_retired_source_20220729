/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.predict;

import karnak.service.Job;
import karnak.service.GlueDatabase;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * DummyRuntimePredictor only use the requested wall time field out of
 * StartTimeQuery objects.
 *
 * The use of the pairs of key and value will be used for the real runtime
 * predictor such as RuntimePredictor.
 *
 */
public class DummyRuntimePredictor extends AbstractRuntimePredictor {

    private static Logger logger = Logger.getLogger(DummyRuntimePredictor.class.getName());

    private static final DummyRuntimePredictor INSTANCE = new DummyRuntimePredictor();

    private DummyRuntimePredictor() {
    }

    public static DummyRuntimePredictor getInstance() {
        return INSTANCE;
    }


    public RuntimePrediction getPrediction(UnsubmittedStartTimeQuery query, Map<String, String> paramsMap) {

        RuntimePrediction runtimePred = new RuntimePrediction();
        runtimePred.setRuntime(query.getRequestedWallTime());
        return runtimePred;
    }


    public RuntimePrediction getPrediction(SubmittedStartTimeQuery query, Map<String, String> paramsMap) {
        RuntimePrediction runtimePred = new RuntimePrediction();

        /*
         Submitted job's requested wall time can be retrieved from database.
         */
        Job job = GlueDatabase.getCurrentJob(query.system, query.jobId);
        if (job == null) {
            logger.error("Failed to get Job object whose id is " + query.jobId);
            return runtimePred;
        }

        /* Uninitialized Job's requestedWallTime is set to -1 */
        if (job.requestedWallTime > 0) {
            /* TODO: Change the access modifier of requestedWallTime to private.
             A getter for the requestedWallTime needs to be added to the Job
             class.
             */
            runtimePred.setRuntime(job.requestedWallTime);
            logger.debug("SetRuntime of Job[" + query.jobId + "] is " + job.requestedWallTime);
        }
        return runtimePred;
    }
}
