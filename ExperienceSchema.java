
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

import java.io.*;
import java.util.*;

public class ExperienceSchema {

    protected List<FeatureSchema> inputs = new ArrayList<FeatureSchema>();  // just order by insertion order
    protected List<FeatureSchema> outputs = new ArrayList<FeatureSchema>();
    protected Map<String,Integer> inputIndex = new HashMap<String,Integer>();
    protected Map<String,Integer> outputIndex = new HashMap<String,Integer>();

    public ExperienceSchema() {
    }

    public ExperienceSchema(ExperienceSchema schema) {
    }


    public void addInput(String name, Class type) {
	addInput(new FeatureSchema(name,type));
    }

    public void addInput(String name, Class type, boolean required) {
	addInput(new FeatureSchema(name,type,required));
    }

    public void addInput(FeatureSchema feature) {
	if (inputIndex.containsKey(feature.getName()) || outputIndex.containsKey(feature.getName())) {
	    System.err.println("feature name '"+feature.getName()+"' already used - not adding feature schema");
	    return;
	}
	inputIndex.put(feature.getName(),inputs.size());
	inputs.add(feature);
    }


    public void addOutput(String name, Class type) {
	addOutput(new FeatureSchema(name,type));
    }

    public void addOutput(String name, Class type, boolean required) {
	addOutput(new FeatureSchema(name,type,required));
    }

    public void addOutput(FeatureSchema feature) {
	if (inputIndex.containsKey(feature.getName()) || outputIndex.containsKey(feature.getName())) {
	    System.err.println("feature name '"+feature.getName()+"' already used - not adding feature schema");
	    return;
	}
	outputIndex.put(feature.getName(),outputs.size());
	outputs.add(feature);
    }

    public int numInputs() {
	return inputs.size();
    }

    public int numOutputs() {
	return outputs.size();
    }

    public FeatureSchema getInput(int index) {
	if ((index < 0) || (index >= inputs.size())) {
	    return null;
	}
	return inputs.get(index);
    }

    public FeatureSchema getOutput(int index) {
	if ((index < 0) || (index >= outputs.size())) {
	    return null;
	}
	return outputs.get(index);
    }

    public FeatureSchema get(String name) {
	if (inputIndex.containsKey(name)) {
	    return inputs.get(inputIndex.get(name));
	}
	if (outputIndex.containsKey(name)) {
	    return outputs.get(outputIndex.get(name));
	}
	return null;
    }

    public int getInputIndex(String name) {
	if (inputIndex.containsKey(name)) {
	    return inputIndex.get(name);
	}
	return -1;
    }

    public int getOutputIndex(String name) {
	if (outputIndex.containsKey(name)) {
	    return outputIndex.get(name);
	}
	return -1;
    }

    public boolean equals(Object obj) {
	if (!(obj instanceof ExperienceSchema)) {
	    return super.equals(obj);
	}
	ExperienceSchema schema = (ExperienceSchema)obj;
	if (!equalFeatures(inputs,schema.inputs)) {
	    return false;
	}
	if (!equalFeatures(outputs,schema.outputs)) {
	    return false;
	}
	return true;
    }

    protected boolean equalFeatures(List<FeatureSchema> schema1, List<FeatureSchema> schema2) {
	if (schema1.size() != schema2.size()) {
	    return false;
	}
	for(int i=0;i<schema1.size();i++) {
	    if (schema1.get(i) == null) {
		if (schema2.get(i) == null) {
		    continue;
		} else {
		    return false;
		}
	    } else {
		if (schema2.get(i) == null) {
		    return false;
		} else {
		    if (!schema1.get(i).equals(schema2.get(i))) {
			return false;
		    }
		}
	    }
	}
	return true;
    }

    public String toString() {
	return toString("");
    }

    public String toString(String indent) {
        String endl = System.getProperty("line.separator"); 
	String str = indent+"ExperienceSchema:"+endl;
	str += indent+"  Inputs:"+endl;
	for(FeatureSchema feature : inputs) {
	    str += feature.toString(indent+"    ");
	}
	str += indent+"  Outputs:"+endl;
	for(FeatureSchema feature : outputs) {
	    str += feature.toString(indent+"    ");
	}
	return str;
    }

}

