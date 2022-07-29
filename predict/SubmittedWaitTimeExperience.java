
package karnak.service.predict;

import java.sql.ResultSet;
import java.text.*;
import java.util.*;
import org.apache.log4j.Logger;

import karnak.learn.*;
import karnak.service.*;

public class SubmittedWaitTimeExperience extends WaitTimeExperience implements Experience {

    private static Logger logger = Logger.getLogger(SubmittedWaitTimeExperience.class.getName());

    public SubmittedWaitTimeExperience() {
    }

    public SubmittedWaitTimeExperience(WaitTimeExperience exp) {
        super(exp);
    }

    public SubmittedWaitTimeExperience(Job job) {
        super(job);
    }

    public SubmittedWaitTimeExperience(ResultSet rs) {
	fromSql(rs);
    }


    protected static ExperienceSchema schema = null;

    static {
	schema = new ExperienceSchema();
	// assuming separate experience base per system (no system feature needed)
	schema.addInput(new FeatureSchema("name",String.class,false));
	schema.addInput(new FeatureSchema("user",String.class,false));
	schema.addInput(new FeatureSchema("project",String.class,false));
	schema.addInput(new FeatureSchema("queue",String.class,true));
	schema.addInput(new FeatureSchema("processors",Integer.class,true));
	schema.addInput(new FeatureSchema("requestedWallTime",Integer.class,true));
	schema.addInput(new FeatureSchema("time",Long.class,true));
	schema.addInput(new FeatureSchema("count",Integer.class,true));
	schema.addInput(new FeatureSchema("work",Long.class,true));
	schema.addInput(new FeatureSchema("userCount",Integer.class,false));
	schema.addInput(new FeatureSchema("userWork",Long.class,false));
	schema.addInput(new FeatureSchema("jobsRunning",Boolean.class,true));
	schema.addInput(new FeatureSchema("simulatedWaitTime",Integer.class,false));
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
                name = (String)value;
            } else if (index == 1) {
                user = (String)value;
            } else if (index == 2) {
                project = (String)value;
            } else if (index == 3) {
                queue = (String)value;
            } else if (index == 4) {
                processors = ((Integer)value).intValue();
            } else if (index == 5) {
                requestedWallTime = ((Integer)value).intValue();
            } else if (index == 6) {
                throw new SchemaException("setting of time feature isn't supported");
            } else if (index == 7) {
                count = ((Integer)value).intValue();
            } else if (index == 8) {
                work = ((Long)value).longValue();
            } else if (index == 9) {
                userCount = ((Integer)value).intValue();
            } else if (index == 10) {
                userWork = ((Long)value).longValue();
            } else if (index == 11) {
                jobsRunning = ((Boolean)value).booleanValue();
            } else if (index == 12) {
                if (time == -1) {
                    throw new SchemaException("can't set simulated wait time - time not set");
                }
                simulatedStartTime = time + ((Integer)value).longValue();
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
            throw new SchemaException("SubmittedWaitTimeExperience does not have an output at index "+index);
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
        if (name.equals("name")) {
            setInput(0,value);
        } else if (name.equals("user")) {
            setInput(1,value);
        } else if (name.equals("project")) {
            setInput(2,value);
        } else if (name.equals("queue")) {
            setInput(3,value);
        } else if (name.equals("processors")) {
            setInput(4,value);
        } else if (name.equals("requestedWallTime")) {
            setInput(5,value);
        } else if (name.equals("time")) {
            setInput(6,value);
        } else if (name.equals("count")) {
            setInput(7,value);
        } else if (name.equals("work")) {
            setInput(8,value);
        } else if (name.equals("userCount")) {
            setInput(9,value);
        } else if (name.equals("userWork")) {
            setInput(10,value);
        } else if (name.equals("jobsRunning")) {
            setInput(11,value);
        } else if (name.equals("simulatedWaitTime")) {
            setInput(12,value);
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
                if (name == null) {
                    return null;
                }
                return new Feature(schema.getInput(index),name);
            } else if (index == 1) {
                if (user == null) {
                    return null;
                }
                return new Feature(schema.getInput(index),user);
            } else if (index == 2) {
                if (project == null) {
                    return null;
                }
                return new Feature(schema.getInput(index),project);
            } else if (index == 3) {
                if (queue == null) {
                    return null;
                }
                return new Feature(schema.getInput(index),queue);
            } else if (index == 4) {
                if (processors == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Integer(processors));
            } else if (index == 5) {
                if (requestedWallTime == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Integer(requestedWallTime));
            } else if (index == 6) {
                if (getTime() == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Long(getTime()));
            } else if (index == 7) {
                if (count == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Integer(count));
            } else if (index == 8) {
                if (work == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Long(work));
            } else if (index == 9) {
                if (userCount == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Integer(userCount));
            } else if (index == 10) {
                if (userWork == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Long(userWork));
            } else if (index == 11) {
                return new Feature(schema.getInput(index),new Boolean(jobsRunning));
            } else if (index == 12) {
                if (time == -1) {
                    return null;
                }
                if (simulatedStartTime == -1) {
                    return null;
                }
                return new Feature(schema.getInput(index),new Integer((int)(simulatedStartTime-time)));
            } else {
                return null;
            }
        } catch (SchemaException e) {
            logger.error("error getting input at index "+index+": "+e.getMessage());
	    logger.error(schema);
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
            logger.error("error getting output: "+e.getMessage());
            return null;
        }
    }

    // from Experience
    public Feature get(String name) {
        if (name.equals("name")) {
            return getInput(0);
        } else if (name.equals("user")) {
            return getInput(1);
        } else if (name.equals("project")) {
            return getInput(2);
        } else if (name.equals("queue")) {
            return getInput(3);
        } else if (name.equals("processors")) {
            return getInput(4);
        } else if (name.equals("requestedWallTime")) {
            return getInput(5);
        } else if (name.equals("time")) {
            return getInput(6);
        } else if (name.equals("count")) {
            return getInput(7);
        } else if (name.equals("work")) {
            return getInput(8);
        } else if (name.equals("userCount")) {
            return getInput(9);
        } else if (name.equals("userWork")) {
            return getInput(10);
        } else if (name.equals("jobsRunning")) {
            return getInput(11);
        } else if (name.equals("simulatedWaitTime")) {
            return getInput(12);
        } else if (name.equals("waitTime")) {
            return getOutput(0);
        } else {
            return null;
        }
    }

    // from Experience
    public void validate() throws SchemaException {
        // check for the required features
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