
package karnak.service.task;

import java.util.Date;

public abstract class PeriodicTask implements Task {

    protected int periodSecs = 300;
    protected Date lastExecutionTime = null;

    public PeriodicTask() {
    }

    public PeriodicTask(int periodSecs) {
	this.periodSecs = periodSecs;
    }

    public boolean ready() {
	if (lastExecutionTime == null) {
	    return true;
	}
	Date curTime = new Date();
	if ((curTime.getTime() - lastExecutionTime.getTime()) > periodSecs * 1000) {
	    return true;
	}
	return false;
    }

    public void run() {
	runTask();
    }

    protected abstract void runTask();
}
