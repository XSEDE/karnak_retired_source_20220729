/****************************************************************************/
/* Copyright 2015 University of Texas                                       */
/*                                                                          */
/* Licensed under the Apache License, Version 2.0 (the "License");          */
/* you may not use this file except in compliance with the License.         */
/* You may obtain a copy of the License at                                  */
/*                                                                          */
/*     http://www.apache.org/licenses/LICENSE-2.0                           */
/*                                                                          */
/* Unless required by applicable law or agreed to in writing, software      */
/* distributed under the License is distributed on an "AS IS" BASIS,        */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and      */
/* limitations under the License.                                           */
/****************************************************************************/

package karnak.service;

import java.util.*;

import org.apache.log4j.Logger;

public class TreeNode {

    private static Logger logger = Logger.getLogger(TreeNode.class.getName());

    protected String name = null;
    protected String nameSpace = null;
    protected Object value = null;
    protected boolean isAttribute = false;
    protected List<TreeNode> children = new Vector<TreeNode>();

    public TreeNode(String name) {
	this.name = name;
    }

    public TreeNode(String name,
		    Object value) {
	this.name = name;
	this.value = value;
    }

    public TreeNode(String name,
		    Object value,
		    String nameSpace) {
	this.name = name;
	this.value = value;
	this.nameSpace = nameSpace;
    }

    public void addAttribute(TreeNode child) {
	child.isAttribute = true;
	children.add(child);
    }

    public void addChild(TreeNode child) {
	child.isAttribute = false;
	children.add(child);
    }

    public String getXml() {
	return getXml("");
    }

    protected String getXml(String indent) {
	if (isAttribute) {
	    return indent+name+"='"+value+"'";
	}

	String xml = indent+"<"+name;
	if (nameSpace != null) {
	    xml += " xmlns='"+nameSpace+"'";
	}
	for(TreeNode node : children) {
	    if (node.isAttribute) {
		xml += node.getXml(" ");
	    }
	}
	xml += ">";
	if (value != null) {
	    xml += value;
	}
	boolean hasChildren = false;
	for(TreeNode node : children) {
	    if (!node.isAttribute) {
		hasChildren = true;
		break;
	    }
	}

	// add a newline if there are any child elements
	if (hasChildren) {
	    xml += "\n";
	}

	for(TreeNode node : children) {
	    if (!node.isAttribute) {
		xml += node.getXml(indent+"    ");
	    }
	}

	// only indent if there are any chldren
	if (hasChildren) {
	    xml += indent;
	}
	xml += "</"+name+">\n";

	return xml;
    }

    public String getJson() {
	String json = "{\n";
	json += getJson("    ",true);
	json += "}\n";
	return json;
    }

    public String getJson(String indent, boolean last) {
	if (children.size() == 0) {
	    String json = "";
	    if (value instanceof String) {
		json += indent+"\""+name+"\" : \""+value+"\"";
	    } else {
		// assuming it is a number of some kind
		json += indent+"\""+name+"\" : "+value+"";
	    }
	    if (!last) {
		json += ",";
	    }
	    json += "\n";
	    return json;
	}

	String json = indent+"\""+name+"\" : [\n";
	for(int i=0;i<children.size();i++) {
	    TreeNode child = children.get(i);
	    if ((i == children.size()-1) && (value == null)) {
		json += child.getJson(indent+"    ",true);
	    } else {
		json += child.getJson(indent+"    ",false);
	    }
	}
	if (value != null) {
	    if (value instanceof String) {
		json += indent+"\""+name+"\" : \""+value+"\"";
	    } else {
		// assuming it is a number of some kind
		json += indent+"\""+name+"\" : "+value+"";
	    }
	}
	json += indent+"]";
	if (!last) {
	    json += ",";
	}
	json += "\n";

	return json;
    }

}
