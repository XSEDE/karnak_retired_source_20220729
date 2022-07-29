
package karnak.service.predict;

import java.sql.ResultSet;
import java.text.*;
import java.util.*;
import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.service.*;

public class UnsubmittedWaitTimeExperience extends WaitTimeExperience implements Experience {

    private static Logger logger = Logger.getLogger(UnsubmittedWaitTimeExperience.class.getName());

    public UnsubmittedWaitTimeExperience() {
    }

    public UnsubmittedWaitTimeExperience(WaitTimeExperience exp) {
        super(exp);
    }

    public UnsubmittedWaitTimeExperience(Job job) {
        super(job);
    }

    public UnsubmittedWaitTimeExperience(ResultSet rs) {
	fromSql(rs);
    }

    protected static ExperienceSchema schema = null;

    // if we had a secure connection, a query could specify a user and could also use userCount and userWork
    // count/work in queue useful?
    static {
	schema = new ExperienceSchema();
	// assuming separate experience base per system (no system feature needed)
	schema.addInput(new FeatureSchema("queue",String.class,true));
	schema.addInput(new FeatureSchema("processors",Integer.class,true));
	schema.addInput(new FeatureSchema("requestedWallTime",Integer.class,true));
	schema.addInput(new FeatureSchema("time",Long.class,true));
	schema.addInput(new FeatureSchema("count",Integer.class,true));
	schema.addInput(new FeatureSchema("work",Long.class,true));
	schema.addInput(new FeatureSchema("jobsRunning",Boolean.class,true));
	schema.addOutput(new FeatureSchema("waitTime",Integer.class,false));
    }

    public static ExperienceSchema getWaitTimeSchema() {
        return schema;
    }

    // from Experience
    public ExperienceSchema getSchema() {
        return schema;
    }

    // from Experience
    public void setInput(int index, Object value) throws SchemaException {
        try {
            if (index == 0) {
                queue = (String)value;
            } else if (index == 1) {
                processors = ((Integer)value).intValue();
            } else if (index == 2) {
                requestedWallTime = ((Integer)value).intValue();
            } else if (index == 3) {
                throw new SchemaException("setting of time feature isn't supported");
            } else if (index == 4) {
                count = ((Integer)value).intValue();
            } else if (index == 5) {
                work = ((Long)value).longValue();
            } else if (index == 6) {
                jobsRunning = ((Boolean)value).booleanValue();
            } else {
                throw new SchemaException("invalid index: "+index);
            }
        } catch (ClassCastException e) {
            throw new SchemaException("wrong type for index "+index+": "+e.getMessage());
        }

    }

    // from Experience
    public void setOutput(int index, Object value) throws SchemaException {
        if (index != 0) {
            throw new SchemaException("UnsubmittedWaitTimeExperience does not have an output at index "+index);
        }

        if (time == -1) {
            throw new SchemaException("time is not set");
        }

        try {
            startTime = time + ((Integer)value).intValue();
        } catch (ClassCastException e) {
            throw new SchemaException("wrong type for index "+index+": "+e.getMessage());
        }
    }

    // from Experience
    public void set(String name, Object value) throws SchemaException {
        if (name.equals("queue")) {
            setInput(0,value);
        } else if (name.equals("processors")) {
            setInput(1,value);
        } else if (name.equals("requestedWallTime")) {
            setInput(2,value);
        } else if (name.equals("time")) {
            setInput(3,value);
        } else if (name.equals("count")) {
            setInput(4,value);
        } else if (name.equals("work")) {
            setInput(5,value);
        } else if (name.equals("jobsRunning")) {
            setInput(6,value);
        } else if (name.equals("waitTime")) {
            setOutput(0,value);
        } else {
            throw new SchemaException("unknown feature "+name);
        }
    }

    // this provides the time as needed for an insert experience
    public long getTime() {
        return startTime;
    }

    // from Experience
    public Feature getInput(int index) {
        try {
            if (index == 0) {
                if (queue == null) {
                    return null;
                }
                return new Feature(schema.getInput(0),queue);
            } else if (index == 1) {
                if (processors == -1) {
                    return null;
                }
                return new Feature(schema.getInput(1),new Integer(processors));
            } else if (index == 2) {
                if (requestedWallTime == -1) {
                    return null;
                }
                return new Feature(schema.getInput(2),new Integer(requestedWallTime));
            } else if (index == 3) {
                if (getTime() == -1) {
                    return null;
                }
                return new Feature(schema.getInput(3),new Long(getTime()));
            } else if (index == 4) {
                if (count == -1) {
                    return null;
                }
                return new Feature(schema.getInput(4),new Integer(count));
            } else if (index == 5) {
                if (work == -1) {
                    return null;
                }
                return new Feature(schema.getInput(5),new Long(work));
            } else if (index == 6) {
                return new Feature(schema.getInput(6),new Boolean(jobsRunning));
            } else {
                return null;
            }
        } catch (SchemaException e) {
            logger.error("error getting input: "+e.getMessage());
            return null;
        }
    }

    // from Experience
    public Feature getOutput(int index) {
        if (index != 0) {
            return null;
        }
        // time better be the time the job will be submitted...
        if (time == -1) {
            return null;
        }
        if (startTime == -1) {
            return null;
        }
        try {
            return new Feature(schema.getOutput(0),new Integer((int)(startTime-time)));
        } catch (SchemaException e) {
            logger.error("error getting input: "+e.getMessage());
            return null;
        }
    }

    // from Experience
    public Feature get(String name) {
        if (name.equals("queue")) {
            return getInput(0);
        } else if (name.equals("processors")) {
            return getInput(1);
        } else if (name.equals("requestedWallTime")) {
            return getInput(2);
        } else if (name.equals("time")) {
            return getInput(3);
        } else if (name.equals("count")) {
            return getInput(4);
        } else if (name.equals("work")) {
            return getInput(5);
        } else if (name.equals("jobsRunning")) {
            return getInput(6);
        } else if (name.equals("waitTime")) {
            return getOutput(0);
        } else {
            return null;
        }
    }

    // from Experience
    public void validate() throws SchemaException {
        if (queue == null) {
            throw new SchemaException("queue not specified");
        }
        if (processors == -1) {
            throw new SchemaException("processors not specified");
        }
        if (requestedWallTime == -1) {
            throw new SchemaException("requested wall time not specified");
        }
        if (getTime() == -1) {
            throw new SchemaException("time not specified");
        }
        if (count == -1) {
            throw new SchemaException("count not specified");
        }
        if (work == -1) {
            throw new SchemaException("work not specified");
        }
        // jobsRunning has a default value, so can't really test it
    }

    // from Experience
    public String toString(String indent) {
        return super.toString(indent);
    }

}