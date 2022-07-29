
package karnak.service.predict;

import org.apache.log4j.Logger;

public class Confidence {

    private static Logger logger = Logger.getLogger(Confidence.class.getName());

    protected String name;
    protected float stdDev;

    public Confidence(String name, float stdDev) {
	this.name = name;
	this.stdDev = stdDev;
	if (stdDev == Float.NaN) {
	    logger.warn("standard deviation is NaN");
	}
    }

    public String getName() {
	return name;
    }

    public void setStdDev(float sd) {
	stdDev = sd;
	if (stdDev == Float.NaN) {
	    logger.warn("standard deviation is NaN");
	}
    }

    public float getStdDev() {
	return stdDev;
    }

    public float getIntervalSize(int levelOfConf) {
	// should we assume that the distribution is normal?
	//   if we do, then use empirical rule:
	return getIntervalSizeEmpiricalRule(levelOfConf);
	// if we don't, then use Tchebysheff's theorem:
	//return getIntervalSizeTchebysheff(levelOfConf);
    }

    protected float getIntervalSizeEmpiricalRule(int levelOfConf) {
	if (stdDev == Float.NaN) {
	    return Float.MAX_VALUE;
	}
	if (levelOfConf >= 95) {
	    return 1.96f * stdDev;
	}
	if (levelOfConf >= 90) {
	    return 1.65f * stdDev;
	}
	if (levelOfConf >= 85) {
	    return 1.44f * stdDev;
	}
	if (levelOfConf >= 80) {
	    return 1.28f * stdDev;
	}
	return 1.15f * stdDev; // 75
    }

    protected float getIntervalSizeTchebysheff(int levelOfConf) {
	if (stdDev == Float.NaN) {
	    return Float.MAX_VALUE;
	}
	float k = (float)Math.sqrt(1/(1-levelOfConf/100.0f));
	return k * stdDev;
    }

    public boolean equals(Object obj) {
	if (!(obj instanceof Confidence)) {
	    return super.equals(obj);
	}
	Confidence conf = (Confidence)obj;
	if (!name.equals(conf.name)) {
	    return false;
	}
	if (stdDev != conf.stdDev) {
	    return false;
	}
	return true;
    }

    public String toString() {
	return "standard deviation for prediction of "+name+" is "+stdDev;
    }

}
