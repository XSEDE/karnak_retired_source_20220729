/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.predict;

import java.util.Date;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * RuntimePrediction keeps the minimal information about runtime. All other
 * information used for runtime prediction can be found in the arguments. For
 * example, the system, queue, jobId, and interval percent are given to the
 * runtime predictor.
 *
 *
 */
public class RuntimePrediction implements Comparable<RuntimePrediction> {

    private long runtime = 0; // in seconds
    private long intervalSecs = 0; // in seconds

    public RuntimePrediction() {

    }

    @Override
    public int compareTo(RuntimePrediction other) {
        return new Long(runtime).compareTo(other.runtime);
    }

    public long getRuntime() {
        return runtime;
    }

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public long getIntervalSecs() {
        return intervalSecs;
    }

    public void setIntervalSecs(long intervalSecs) {
        this.intervalSecs = intervalSecs;
    }

}
