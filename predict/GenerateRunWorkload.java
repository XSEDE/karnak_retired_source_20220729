
package karnak.service.predict;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;

import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.learn.trace.*;
import karnak.service.*;

public class GenerateRunWorkload extends Service {

    private static Logger logger = Logger.getLogger(GenerateRunWorkload.class.getName());

    public static void main(String[] args) {
	if (args.length != 3) {
	    System.err.println("usage: GenerateRunWorkload <system> <start> <end>");
	    System.err.println("         date/time format is year-month-day (time is assumed to be 00:00:00 GMT)");
	    System.exit(1);
	}

	long start = dateToEpoch(args[1]);
	long end = dateToEpoch(args[2]);

	String workloadDir = "/home/karnak/workloads/teragrid/runTime/";
	StringTokenizer tok = new StringTokenizer(args[0],".");
	String shortName = tok.nextToken();

	GenerateRunWorkload gw = new GenerateRunWorkload();
	try {
	    gw.generate(workloadDir+shortName+"_"+args[1]+"_"+args[2]+".exp",args[0],start,end);
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    public GenerateRunWorkload() {
	stopRunning();
    }

    protected void generate(String fileName, String system, long start, long end) {
	logger.info("generating run time experiences");

	// this method returns jobs with start times between start and end
	List<Job> jobs = readStartedJobs(system,start,end);
	logger.info("  found "+jobs.size()+" jobs that started");

	TreeSet<RunTimeExperience> experiences = new TreeSet<RunTimeExperience>(new TimeComparator());
	for(Job job : jobs) {
	    insertExperiences(job,start,end,experiences);
	}
	logger.info("  writing "+experiences.size()+" experiences");

	writeExperiences(experiences,fileName);
    }

    protected void insertExperiences(Job job,
                                     long earliestTime, long latestTime,
                                     TreeSet<RunTimeExperience> experiences) {
	if (job.getEndTime() == -1) {
	    return;
	}
	if ((job.getSubmitTime() >= earliestTime) && (job.getSubmitTime() <= latestTime)) {
	    RunTimeExperience exp = new RunTimePrediction(job);
	    try {
		exp.validate();
		experiences.add(exp); // only add if it is valid
	    } catch (SchemaException e) {
		logger.warn(e.getMessage()+": "+exp);
	    }
	}
	if ((job.getEndTime() >= earliestTime) && (job.getEndTime() <= latestTime)) {
	    RunTimeExperience exp = new RunTimeExperience(job);
	    try {
		exp.validate();
		experiences.add(exp); // only add if it is valid
	    } catch (SchemaException e) {
		logger.warn(e.getMessage()+": "+exp);
	    }
	}
    }

    protected void writeExperiences(TreeSet<RunTimeExperience> experiences, String fileName) {
	try {
	    SimpleExperienceWriter writer = new SimpleExperienceWriter(RunTimeExperience.getRunTimeSchema(),
								       new FileWriter(fileName));
	    for(Experience exp : experiences) {
		writer.write(exp);
	    }
	    writer.close();
	} catch (IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    class TimeComparator implements Comparator<RunTimeExperience> {

        public TimeComparator() {
        }

        public int compare(RunTimeExperience exp1, RunTimeExperience exp2) {
            int diff = (int)(exp1.getTime()-exp2.getTime());
            if (diff != 0) {
                return diff;
            }
            if (exp1 instanceof Prediction) {
                if (!(exp2 instanceof Prediction)) {
                    return 1;
                }
            } else {
                if (exp2 instanceof Prediction) {
                    return -1;
                }
            }
            return exp1.hashCode() - exp2.hashCode();
        }
    }

}
