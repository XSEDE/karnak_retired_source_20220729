package karnak.service.predict;
import java.util.Date;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



/**
 *
 * @author wooj
 * 
 * FinishTimePrediction was designed to be used for predicting finish time.
 * Since the finish time can be easily computed by summing the start time and
 * the run time, runtime prediction is used instead. 
 * 
 * As of 2/11/2016, the FinishTimePrediction should not be used any longer.
 * Please see RuntimePrediction 
 * 
 */
@Deprecated
public class FinishTimePrediction extends FinishTimeQuery implements Predictable<FinishTimePrediction> {
    public Date finishTime = null;
    public long intervalSecs = 0;

    @Deprecated
    public FinishTimePrediction(FinishTimeQuery query) {
	system = query.system;
	queue = query.queue;
	processors = query.processors;
	requestedWallTime = query.requestedWallTime;
	intervalPercent = query.intervalPercent;
        jobId = query.jobId;
    }
    
    /* Jungha Woo
       1/26/2016
       Multiple predictions need to be sorted by startTime in increasing order
    for grouping systems.
    */
    public int compareTo(FinishTimePrediction other){
        return finishTime.compareTo(other.finishTime);
    }
    
    public Date getPredictedTime(){
        return finishTime;
    }


    public String getSystemName(){
        return system;
    };

    public String getQueueName(){
        return queue;
    };

    /* Class that does not have jobId field must return error messsage
       or 
    */
    public String getJobId(){
        return jobId;
    };

    public int getNumProcessors(){
        return processors;
    };

    public int getRequestedWallTime(){
        return requestedWallTime;
    };

    public int getIntervalPercent(){
        return intervalPercent;
    };
    
    public long getIntervalSecs(){
        return intervalSecs;
    };
}
