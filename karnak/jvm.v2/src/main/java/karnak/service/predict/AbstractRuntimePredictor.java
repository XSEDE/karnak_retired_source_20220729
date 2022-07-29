/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.predict;
import java.util.Map;

 /**
         *
         * @author Jungha Woo <wooj@purdue.edu>
         *
         * AbstractRuntimePredictor is an abstract class defining behaviors for
         * the any predictor that estimates the running time of submitted jobs.
         * The runtime estimates are largely dependent on the parameters to the
         * application and less dependent on the queue or systems status.
         *
         * DummyRuntimePredictor will inherit from this abstract class.
         * It simply returns requested wall time as estimated run time. 
         * This abstract class may be
         * rewritten in scala once the real runtime predictor is developed.
         *
         *
         * Design consideration 1. It must be able to handle both Submitted and
         * Unsubmitted jobs' runtime. Therefore the parameters to the
         * constructor should be able to accept both types. 2. Constructors of
         * the real implementation classes should require application specific
         * parameters that are not in the UnsubmittedStartTimeQuery or
         * SubmittedStartTimeQuery. 3. The application specific parameters vary
         * from application to application. so somehow it should handle variable
         * number of parameters. To this end, a map composed of key and value pairs
         * is passed to the getPrediction method.
         *
         *
         */

abstract class AbstractRuntimePredictor {

    public abstract  RuntimePrediction getPrediction(UnsubmittedStartTimeQuery query, Map<String, String> paramsMap);

    // takes submitted job
    public abstract  RuntimePrediction getPrediction(SubmittedStartTimeQuery query, Map<String, String> paramsMap);

}


