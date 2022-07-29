
package karnak.service.task;

import java.util.*;

public class TaskThread extends Thread {

    protected Vector<Task> tasks = new Vector<Task>();
    protected boolean keepRunning = true;

    public TaskThread() {
    }

    public void stopRunning() {
	keepRunning = false;
    }

    public void add(Task task) {
	synchronized(tasks) {
	    tasks.add(task);
	}
    }

    public void remove(Task task) {
	synchronized(tasks) {
	    tasks.remove(task);
	}
    }

    public void run() {
	while(keepRunning) {
	    synchronized(tasks) {
		for(Task task : tasks) {
		    if (task.ready()) {
			task.run();
		    }
		}
	    }
	    try {
		Thread.sleep(5*1000);
	    } catch (InterruptedException e) {}
	}
    }
}
