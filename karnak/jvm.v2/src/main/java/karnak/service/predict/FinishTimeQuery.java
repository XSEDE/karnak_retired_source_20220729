/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package karnak.service.predict;

/**
 *
 * @author wooj
 * This class saves the query parameters.
 * The finish time may be estimated for either submitted or unsubmitted jobs.
 * Therefore FinishTimeQuery has merged set of parameters of SubmittedStartTime-
 * Prediction and 
 * unsubmittedStartTimePrediction.
 * 
 */
public class FinishTimeQuery {
    public String system = null;
    public String queue = null;
    public int processors = 0;
    public int requestedWallTime = 0; // seconds
    public int intervalPercent = 90;
    public String jobId = null;
}
