
/**********************************************************************

Copyright 2008 Warren Smith

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0 

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

**********************************************************************/

package karnak.learn;

import java.io.Serializable;
import java.util.*;

public class Feature {

    protected FeatureSchema schema = null;
    //protected String name = null;

    protected Object value = null;

    public Feature(FeatureSchema schema) {
	this.schema = schema;
    }

    public Feature(FeatureSchema schema, Object value) throws SchemaException {
	this.schema = schema;
	setValue(value);
    }

    public Feature(Feature feature) {
	schema = feature.schema;
	value = feature.value;
    }

    public boolean equals(Object obj) {
	if (!(obj instanceof Feature)) {
	    return super.equals(obj);
	}
	Feature feature = (Feature)obj;

	if (!schema.equals(feature.schema)) {
	    return false;
	}

	if (value == null) {
	    if (feature.value == null) {
		return true;
	    } else {
		return false;
	    }
	} else {
	    if (feature.value == null) {
		return false;
	    } else {
		return value.equals(feature.value);
	    }
	}
    }

    public String toString() {
        return toString("");
    }

    public String toString(String indent) {
        String endl = System.getProperty("line.separator");
	if (value == null) {
	    return indent+schema.getName()+": (null)"+endl;
	}
	return indent+schema.getName()+": "+getStringValue()+" ("+value.getClass().getName()+")"+endl;
    }

    public String getName() {
	return schema.getName();
    }

    public Class getType() {
	return schema.getType();
    }

    public void setValue(Object value) throws SchemaException {
	if (value.getClass() != schema.getType()) {
	    throw new SchemaException("feature "+schema.getName()+" is of type "+value.getClass().getName()+
				      " and should be of type "+schema.getType().getName());
	}
	this.value = value;
    }

    public Object getValue() {
	return value;
    }

    public String getStringValue() {
	if (value == null) {
	    return null;
	}
	return value.toString();
    }

    public boolean getBooleanValue() throws ClassCastException {
	return (Boolean)value;
    }

    public int getIntegerValue() throws ClassCastException {
	return (Integer)value;
    }

    public long getLongValue() throws ClassCastException {
	try {
	    return ((Long)value).longValue();
	} catch (ClassCastException e) {}
	return ((Integer)value).intValue();
    }

    public float getFloatValue() throws ClassCastException {
	try {
	    return ((Float)value).floatValue();
	} catch (ClassCastException e) {}
	try {
	    return ((Long)value).longValue();
	} catch (ClassCastException e) {}
	return ((Integer)value).intValue();
    }

}
