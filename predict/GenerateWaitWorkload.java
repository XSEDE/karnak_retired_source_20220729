
package karnak.service.predict;

import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.*;

import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.learn.trace.*;
import karnak.service.*;

public class GenerateWaitWorkload extends Service {

    private static Logger logger = Logger.getLogger(GenerateWaitWorkload.class.getName());

    public static void main(String[] args) {
	if (args.length != 3) {
	    System.err.println("usage: GenerateWaitWorkload <system> <start> <end>");
	    System.err.println("         date/time format is year-month-day (time is assumed to be 00:00:00 GMT)");
	    System.exit(1);
	}

	long start = dateToEpoch(args[1]);
	long end = dateToEpoch(args[2]);

	String workloadDir = "/home/karnak/workloads/teragrid/waitTime/";
	StringTokenizer tok = new StringTokenizer(args[0],".");
	String shortName = tok.nextToken();

	System.out.println("assuming that all experiences are in the database...");
	GenerateWaitWorkload gw = new GenerateWaitWorkload();
	try {
	    gw.generateUnsubmitted(workloadDir+shortName+"_unsubmitted_"+args[1]+"_"+args[2]+".exp",args[0],start,end);
	    gw.generateSubmitted(workloadDir+shortName+"_submitted_"+args[1]+"_"+args[2]+".exp",args[0],start,end);
	} catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    public GenerateWaitWorkload() {
	stopRunning();
    }

    protected void generateUnsubmitted(String fileName, String system, long start, long end) {
	logger.info("generating unsubmitted experiences");

	List<UnsubmittedWaitTimeExperience> experiences = readUnsubmitted(system,start,end);
	logger.info("  "+experiences.size()+" experiences found");

	TreeSet<UnsubmittedWaitTimeExperience> sortedExperiences =
            new TreeSet<UnsubmittedWaitTimeExperience>(new TimeComparator());
	for(UnsubmittedWaitTimeExperience exp : experiences) {
	    insertExperiences(exp,start,end,sortedExperiences);
	}

	int numPred = 0;
	for(UnsubmittedWaitTimeExperience exp : sortedExperiences) {
	    if (exp instanceof UnsubmittedWaitTimePrediction) {
		numPred++;
	    }
	}
	logger.info("  "+sortedExperiences.size()+" to write ("+numPred+" predict, "+
		    (sortedExperiences.size()-numPred)+" insert)");

	writeUnsubmittedExperiences(sortedExperiences,fileName);
    }

    protected void generateSubmitted(String fileName, String system, long start, long end) {
	logger.info("generating submitted experiences");

	List<SubmittedWaitTimeExperience> experiences = readSubmitted(system,start,end);
	logger.info("  "+experiences.size()+" experiences found");

	TreeSet<String> jobIds = new TreeSet<String>();
	for(SubmittedWaitTimeExperience exp : experiences) {
	    jobIds.add(exp.id);
	}
	logger.info("  "+jobIds.size()+" unique job ids");

	TreeSet<SubmittedWaitTimeExperience> sortedExperiences =
            new TreeSet<SubmittedWaitTimeExperience>(new TimeComparator());
	for(SubmittedWaitTimeExperience exp : experiences) {
	    insertExperiences(exp,start,end,sortedExperiences);
	}

	int numPred = 0;
	for(SubmittedWaitTimeExperience exp : sortedExperiences) {
	    if (exp instanceof SubmittedWaitTimePrediction) {
		numPred++;
	    }
	}
	logger.info("  "+sortedExperiences.size()+" to write ("+numPred+" predict, "+
		    (sortedExperiences.size()-numPred)+" insert)");
	logger.info("    "+tooEarly+" experiences were too early");
	logger.info("    "+tooLate+" experiences were too late");

	writeSubmittedExperiences(sortedExperiences,fileName);
    }

    protected List<UnsubmittedWaitTimeExperience> readUnsubmitted(String system, long start, long end) {
	Vector<UnsubmittedWaitTimeExperience> experiences = new Vector<UnsubmittedWaitTimeExperience>();
	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();
	    // time is the prediction time, startTime is the insertion time
	    ResultSet rs = stat.executeQuery("select * from unsubmitted_experiences where system = '"+system+"'"+
					     " and ((time >= "+start+" and time < "+end+")"+
					     " or (startTime >= "+start+" and startTime < "+end+"))");
	    while (rs.next()) {
		UnsubmittedWaitTimeExperience exp = new UnsubmittedWaitTimeExperience();
		exp.fromSql(rs);
		experiences.addElement(exp);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
	return experiences;
    }

    protected List<SubmittedWaitTimeExperience> readSubmitted(String system, long start, long end) {
	Vector<SubmittedWaitTimeExperience> experiences = new Vector<SubmittedWaitTimeExperience>();

	try {
	    Connection conn = getKarnakConnection();
	    Statement stat = conn.createStatement();
	    // time is the prediction time, startTime is the insertion time
	    ResultSet rs = stat.executeQuery("select * from submitted_experiences where system = '"+system+"'"+
					     " and ((time >= "+start+" and time < "+end+")"+
					     " or (startTime >= "+start+" and startTime < "+end+"))");
	    /*
	    ResultSet rs = stat.executeQuery("select * from submitted_experiences where system = '"+system+"'"+
					     " and (startTime >= "+start+" and startTime < "+end+")");
	    */
	    while (rs.next()) {
		SubmittedWaitTimeExperience exp = new SubmittedWaitTimeExperience();
		exp.fromSql(rs);
		experiences.addElement(exp);
	    }
	    rs.close();
	    stat.close();
	    conn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	    System.exit(1);
	}

	return experiences;
    }

    protected void insertExperiences(UnsubmittedWaitTimeExperience exp,
                                     long earliestTime, long latestTime,
                                     TreeSet<UnsubmittedWaitTimeExperience> experiences) {
	if ((exp.getTime() >= earliestTime) && (exp.getTime() < latestTime)) {
	    experiences.add(new UnsubmittedWaitTimeExperience(exp));
	}
	UnsubmittedWaitTimePrediction pred = new UnsubmittedWaitTimePrediction(exp);
	if ((pred.getTime() >= earliestTime) && (pred.getTime() < latestTime)) {
	    experiences.add(pred);
	}
    }

    protected int tooEarly = 0;
    protected int tooLate = 0;

    protected void insertExperiences(SubmittedWaitTimeExperience exp,
                                     long earliestTime, long latestTime,
                                     TreeSet<SubmittedWaitTimeExperience> experiences) {
	if (exp.getTime() < earliestTime) {
	    System.out.println("time "+exp.getTime()+" is early");
	    tooEarly++;
	} else if (exp.getTime() > latestTime) {
	    tooLate++;
	} else {
	    experiences.add(exp);
	}
	/*
	if ((exp.getTime() >= earliestTime) && (exp.getTime() < latestTime)) {
	    experiences.add(new SubmittedWaitTimeExperience(exp));
	}
	*/
	SubmittedWaitTimePrediction pred = new SubmittedWaitTimePrediction(exp);
	if ((pred.getTime() >= earliestTime) && (pred.getTime() < latestTime)) {
	    experiences.add(pred);
	}
    }

    protected void writeUnsubmittedExperiences(TreeSet<UnsubmittedWaitTimeExperience> experiences, String fileName) {
        ExperienceSchema schema = UnsubmittedWaitTimeExperience.getWaitTimeSchema();
	try {
	    SimpleExperienceWriter writer = new SimpleExperienceWriter(schema,new FileWriter(fileName));
	    for(Experience exp : experiences) {
		try {
		    writer.write(exp);
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	    writer.close();
	} catch (IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    protected void writeSubmittedExperiences(TreeSet<SubmittedWaitTimeExperience> experiences, String fileName) {
        ExperienceSchema schema = SubmittedWaitTimeExperience.getWaitTimeSchema();
	try {
	    SimpleExperienceWriter writer = new SimpleExperienceWriter(schema,new FileWriter(fileName));
	    for(Experience exp : experiences) {
		writer.write(exp);
	    }
	    writer.close();
	} catch (IOException e) {
	    e.printStackTrace();
	    System.exit(1);
	}
    }

    class TimeComparator implements Comparator<WaitTimeExperience> {

        public TimeComparator() {
        }

        public int compare(WaitTimeExperience exp1, WaitTimeExperience exp2) {
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

