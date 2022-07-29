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

public class Table {

    private static Logger logger = Logger.getLogger(Table.class.getName());

    public static int JUSTIFICATION_RIGHT = 1;
    public static int JUSTIFICATION_CENTER = 2;
    public static int JUSTIFICATION_LEFT = 3;

    protected String caption = "";
    protected List<Integer> dataJustification = null;
    protected String indent = "";
    protected List<String> header = null;
    protected List<List<String>> data = new ArrayList<List<String>>();
    protected List<String> footer = null;
    protected int numColumns = 1;

    public Table(String caption) {
	this.caption = caption;
    }

    public void setIndent(int indent) {
	this.indent = String.format("%"+indent+"s","");
    }

    public void setHeader(String... values) {
	header = new ArrayList<String>();
	for(String value : values) {
	    header.add(value);
	}
	dataJustification = new ArrayList<Integer>();
	for(int i=0;i<header.size();i++) {
	    dataJustification.add(JUSTIFICATION_RIGHT);
	}
    }

    public void setDataJustification(int... justifications) {
	int column = 0;
	for(int justification : justifications) {
	    dataJustification.set(column,justification);
	    column++;
	    if (column >= dataJustification.size()) {
		return;
	    }
	}
    }

    public void addRow(String... values) {
	if (header == null) {
	    logger.error("specify a header before adding rows");
	    return;
	}
	List<String> row = new ArrayList<String>();
	for(String value : values) {
	    row.add(value);
	}
	if (row.size() != header.size()) {
	    logger.error("row has "+row.size()+" values but table has "+header.size()+"columns");
	    return;
	}
	data.add(row);
    }
    
    public void setFooter(String... values) {
	if (header == null) {
	    logger.error("specify a header before a footer");
	    return;
	}
	footer = new ArrayList<String>();
	for(String value : values) {
	    footer.add(value);
	}
	if (footer.size() != header.size()) {
	    logger.error("footer has "+footer.size()+" values but table has "+header.size()+"columns");
	    footer = null;
	    return;
	}
    }

    public String getText() {
	List<Integer> columnWidths = calculateColumnWidths();
	List<Integer> dataWidths = calculateDataWidths();
	int totalWidth = calculateTotalWidth(columnWidths);

	String txt = getCaptionText(totalWidth);
	txt += getHeaderText(columnWidths);
	txt += getSeparatorText(totalWidth);
	txt += getRowsText(columnWidths,dataWidths);
	if (footer != null) {
	    txt += getSeparatorText(totalWidth);
	    txt += getFooterText(columnWidths,dataWidths);
	}

	return txt;
    }

    protected List<Integer> calculateColumnWidths() {
	List<Integer> width = calculateDataWidths();

	for(int i=0;i<header.size();i++) {
	    String[] toks = header.get(i).split("\n");
	    int maxLen = 0;
	    for(String tok : toks) {
		if (tok.length() > maxLen) {
		    maxLen = tok.length();
		}
	    }
	    if (maxLen> width.get(i)) {
		width.set(i,maxLen);
	    }
	}

	return width;
    }


    protected List<Integer> calculateDataWidths() {
	List<Integer> width = new ArrayList<Integer>();

	for(int i=0;i<header.size();i++) {
	    width.add(0);
	}

	for(List<String> row : data) {
	    for(int i=0;i<row.size();i++) {
		if (row.get(i).length() > width.get(i)) {
		    width.set(i,row.get(i).length());
		}
	    }
	}

	if (footer != null) {
	    for(int i=0;i<footer.size();i++) {
		if (footer.get(i).length() > width.get(i)) {
		    width.set(i,footer.get(i).length());
		}
	    }
	}

	return width;
    }

    protected int calculateTotalWidth(List<Integer> columnWidths) {
	int totalWidth = 0;
	for(Integer colWidth : columnWidths) {
	    totalWidth += colWidth;
	}
	totalWidth += (columnWidths.size() - 1) * 2;
	return totalWidth;
    }

    public int getTotalWidth() {
	return calculateTotalWidth(calculateColumnWidths());
    }

    protected String getCaptionText(int totalWidth) {
	String str = indent;
	int leftSize = (totalWidth-2-caption.length())/2;
	int rightSize = (totalWidth-2-caption.length())/2;
	if ((totalWidth-2-caption.length()) % 2 == 1) {
	    rightSize++;
	}
	for(int i=0;i<leftSize;i++) {
	    str += "-";
	}
	str += " " + caption + " ";
	for(int i=0;i<rightSize;i++) {
	    str += "-";
	}
	str += "\n";
	return str;
    }

    protected String getSeparatorText(int totalWidth) {
	String str = indent;
	for(int i=0;i<totalWidth;i++) {
	    str += "-";
	}
	str += "\n";
	return str;
    }

    protected String getHeaderText(List<Integer> columnWidths) {
	List<Integer> just = new ArrayList<Integer>();
	for(int i=0;i<header.size();i++) {
	    just.add(JUSTIFICATION_CENTER);
	}
	return getRowText(header,columnWidths,columnWidths,just);
    }

    protected String getFooterText(List<Integer> columnWidths, List<Integer> dataWidths) {
	return getRowText(footer,columnWidths,dataWidths,dataJustification);
    }

    protected String getRowsText(List<Integer> columnWidths, List<Integer> dataWidths) {
	String txt = "";
	for(List<String> row : data) {
	    txt += getRowText(row,columnWidths,dataWidths,dataJustification);
	}
	return txt;
    }

    protected String getRowText(List<String> values,
				List<Integer> totalColumnWidths,
				List<Integer> columnWidths,
				List<Integer> justification) {
	int maxLines = calculateMaxLines(values);
	String str = indent;
	for(int i=0;i<maxLines;i++) {
	    str += getOneLineRowText(getLine(i,values),totalColumnWidths,columnWidths,justification);
	}
	return str;
    }

    protected String getOneLineRowText(List<String> values,
				       List<Integer> totalColumnWidths,
				       List<Integer> columnWidths,
				       List<Integer> justification) {
	String formatStr = "";
	for(int i=0;i<values.size();i++) {
	    if (i != 0) {
		formatStr += "  ";
	    }

	    int extraSpaces = totalColumnWidths.get(i) - columnWidths.get(i);
	    for(int j=0;j<extraSpaces/2;j++) {
		formatStr += " ";
	    }
	    
	    if (justification.get(i) == JUSTIFICATION_LEFT) {
		formatStr += "%-"+columnWidths.get(i)+"s";
	    } else if (justification.get(i) == JUSTIFICATION_RIGHT) {
		formatStr += "%"+columnWidths.get(i)+"s";
	    } else if (justification.get(i) == JUSTIFICATION_CENTER) {
		int leftSpaces = (columnWidths.get(i) - values.get(i).length()) / 2;
		for(int j=0;j<leftSpaces;j++) {
		    formatStr += " ";
		}
		formatStr += "%-"+(columnWidths.get(i)-leftSpaces)+"s";
	    } else {
		logger.warn("unknown justification for column "+i+": "+justification.get(i));
		formatStr += "%"+columnWidths.get(i)+"s";
	    }

	    for(int j=extraSpaces/2;j<extraSpaces;j++) {
		formatStr += " ";
	    }

	    if (i==values.size()-1) {
		formatStr += "\n";
	    }
	}
	return String.format(formatStr,values.toArray());
    }

    protected int calculateMaxLines(List<String> values) {
	int maxLines = 0;
	for(String value : values) {
	    String[] toks = value.split("\n");
	    if (toks.length > maxLines) {
		maxLines = toks.length;
	    }
	}
	return maxLines;
    }

    protected List<String> getLine(int lineNumber, List<String> values) {
	List<String> line = new ArrayList<String>();
	for(String value : values) {
	    String[] toks = value.split("\n");
	    if (lineNumber >= toks.length) {
		line.add("");
	    } else {
		line.add(toks[lineNumber]);
	    }
	}
	return line;
    }

    public String getHtml() {
	String html = getCaptionHtml();
	html += getHeaderHtml();
	html += getRowsHtml();
	html += getFooterHtml();
	return html;

	//return "";
    }

    protected String getCaptionHtml() {
	String str = "<title>"+caption+"</title>\n";
	str = str +"<h1 align='center'>"+caption+"</h1>\n";
	return str;
    }

    protected String getHeaderHtml() {
	String str = "<p><table border='1' align='center'>\n";
	str += "<tr>\n";
	for(int i=0;i<header.size();i++) {
	    str += "<th>"+header.get(i)+"</th>\n";
	}
	str += "</tr>\n";
	return str;
    }

    protected String getFooterHtml() {
	String str = "<p><table border='1' align='center'>\n";
	if (footer != null) {
	    str += "<tr>\n";
	    for(int i=0;i<footer.size();i++) {
		str += "<th>"+footer.get(i)+"</th>\n";
	    }
	    str += "</tr>\n";
	}
	str += "</table>\n";
	return str;
    }

    protected String getRowsHtml() {
	String str = "";
	for(List<String> row : data) {
	    str += getRowHtml(row);
	}
	return str;
    }

    protected String getRowHtml(List<String> values) {
	String str = "<tr>";
	for(int i=0;i<values.size();i++) {
	    str += "<td";
	    if (dataJustification.get(i) == JUSTIFICATION_LEFT) {
		str += " align='left'";
	    } else if (dataJustification.get(i) == JUSTIFICATION_RIGHT) {
		str += " align='right'";
	    } else if (dataJustification.get(i) == JUSTIFICATION_CENTER) {
		str += " align='center'";
	    } else {
		logger.warn("unknown justification for column "+i+": "+dataJustification.get(i));
		str += " align='center'";
	    }
	    str += ">";
	    str += values.get(i).replaceAll("\\n"," <br> ");
	    str += "</td>\n";
	}
	str += "</tr>\n";
	return str;
    }

    /**************************************************************************************************************/

    public static void test1() {
	Table table = new Table("System Status");
	table.setHeader("System Name","Running Jobs","Waiting Jobs","Used Processors");

	table.addRow("system1.site1.teragrid.org","123","45","6789");
	table.addRow("system2.site1.teragrid.org","0","1234","567890");
	table.addRow("system1.othersite.teragrid.org","unknown","unknown","unknown");

        System.out.println(table.getText());
    }

    public static void test2() {
	Table table = new Table("System "+"system.site.teragrid.org");
	table.setHeader("Queue Name","Running Jobs","Waiting Jobs","Used Processors");

	table.addRow("development","10","8","600");
	table.addRow("long","150","120","17300");
	table.addRow("normal","200","300","43100");
	table.setFooter("all_jobs","460","428","61000");

        System.out.println(table.getText());
    }

    public static void test3() {
	Table table1 = new Table("Status");
	table1.setHeader("Running Jobs","Waiting Jobs","Used Processors");
	table1.addRow("460","428","61000");

	Table table2 = new Table("Started Jobs");
	table2.setHeader("When",
			 "Number of\nJobs",
			 "Mean\nProcessors",
			 "Mean Requested Wall Time\n(hours:minutes:seconds)",
			 "Mean Wait Time\n(hours:minutes:seconds)");
	table2.addRow("last hour","64","172","14:40:17","03:24:36");
	table2.addRow("last four hours","166","207","11:25:31","03:43:02");
	table2.addRow("last day","1040","164","16:13:58","05:54:39");
	table2.addRow("last week","6872","300","14:08:51","12:30:39");

	table1.setIndent((table2.getTotalWidth()-table1.getTotalWidth())/2);
	
        System.out.println(table1.getText());
	System.out.println();
        System.out.println(table2.getText());
    }

    public static void main(String[] argv) {
	//test1();
	//test2();
	test3();
    }

}
