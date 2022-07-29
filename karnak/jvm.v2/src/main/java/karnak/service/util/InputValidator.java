/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.util;

import org.apache.log4j.Logger;
import karnak.service.predict.UnsubmittedStartTimePrediction;
import karnak.service.predict.SubmittedStartTimePrediction;
import karnak.service.WebService;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * returns true if the prediction object is valid object and ready to be used
 * for the prediction. False will be return if the input is not appropriate
 * after setting error message to the input object, prediction.
 *
 *
 * Please note that the prediction object is NOT the actual prediction output
 * yet. if a prediction object does not have error, it is ready to be used by
 * predictor to fill in prediction ( start time or wait time)
 *
 */
public class InputValidator {

    private static Logger logger = Logger.getLogger(InputValidator.class.getName());

    /* Here we validate the input parameters.
     If the users' requirements ( # of processors, and max walltime) 
     cannot be met by queue@system, 
     error message will be set to the prediction object and 
     continue to the next queue@system.
                
     first, non existing system name or queue name may be received
     due to typo or incorrect knowledge.
     if non existing system name is received, set error message and
     go on to the next request.
     */
    public static boolean validate(UnsubmittedStartTimePrediction prediction) {

        //checks if users want to use existing system
        if (SystemInfoQuery.isValidSystem(prediction.system) == false) {
            prediction.setErrorMessage("Unknown system");
            logger.debug(prediction.getErrorMessage());
            return false;
        }

        //checks if users want to use existing queue
        if (SystemInfoQuery.isValidQueue(prediction.system, prediction.queue) == false) {
            prediction.setErrorMessage("Unknown queue");
            logger.debug(prediction.getErrorMessage());
            return false;
        }

        int maxProcs = SystemInfoQuery.getMaxProcessors(prediction.system, prediction.queue);
        if (prediction.processors > maxProcs) {
            prediction.setErrorMessage("requested processors must be less than " + String.valueOf(maxProcs));
            logger.debug(prediction.getErrorMessage());
            return false;
        }

        //requested wall time must be smaller than max allowed wall time
        logger.debug("Systeminfoquery getMaxWallTime for system:" + prediction.system + " queue:" + prediction.queue);
        int maxWallTime = SystemInfoQuery.getMaxWallTime(prediction.system, prediction.queue);
        if (prediction.requestedWallTime > maxWallTime) {
            prediction.setErrorMessage("requested wall time must be less than " + WebService.hms(maxWallTime));
            logger.debug(prediction.getErrorMessage());
            return false;
        }

        return true;
    }

    /* Submitted Jobs already met the requirements for the queue and system 
     therefore some checks are not necessary.
     */
    public static boolean validate(SubmittedStartTimePrediction prediction) {

        //checks if users want to use existing system
        if (SystemInfoQuery.isValidSystem(prediction.system) == false) {
            prediction.setErrorMessage("Unknown system");
            logger.debug(prediction.getErrorMessage());
            return false;
        }

        return true;
    }

}
