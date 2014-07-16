package ToWekaForm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class ToWekaMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	
    @Override
    
    public void map(LongWritable key, Text value, 
                      OutputCollector<Text, Text> output, 
                      Reporter reporter) throws IOException {

    	String line = value.toString();
    	
    	//基本单条记录
    	String time = "";
    	String gp = "";
    	String site = "";
    	String url = "";
    	String Type = "";
    	String size = "";
    	String referer = "";
    	String cip = "";
    	String cport = "";
    	String sip = "";
    	String sport = "";
    	String agent = "";
    	//回包
    	String returnTime = "";
    	String statusCode = "";
    	String unknown1 = "";
    	String unknown2 = "";
    	String returnType = "";
    	String returnSize = "";
    	String unknown3 = "";
    	String returnAgent = "";
    	//单条特征
    	String depth = "";
    	String parameterNumber = "";
    	String hasAgent = "";
    	String isAgentProgram = "";
    	//session特征
    	String visitTime = "";
    	String width = "";
    	String getRate = "";
    	String staticPageRate = "";
    	String statusCode2xxRate = "";
    	
    	try {
	    	String[] temp = line.split(",", -1);
	    	
	    	time = temp[0];
	    	gp = temp[1];
	    	site = temp[2];
	    	url = temp[3];
	    	Type = temp[4];
	    	size = temp[5];
	    	referer = temp[6];
	    	cip = temp[7];
	    	cport = temp[8];
	    	sip = temp[9];
	    	sport = temp[10];
	    	agent = temp[11];
	    	returnTime = temp[12];
	    	statusCode = temp[13];
	    	unknown1 = temp[14];
	    	unknown2 = temp[15];
	    	returnType = temp[16];
	    	returnSize = temp[17];
	    	unknown3 = temp[18];
	    	returnAgent = temp[19];
	    	depth = temp[20];
	    	parameterNumber = temp[21];
	    	hasAgent = temp[22];
	    	isAgentProgram = temp[23];
	    	visitTime = temp[24];
	    	width = temp[25];
	    	getRate = temp[26];
	    	staticPageRate = temp[27];
	    	statusCode2xxRate = temp[28];
    	}
    	catch (Exception e) {
    		;
    	}

    	String attributes = depth + "," + parameterNumber + "," + hasAgent + "," + isAgentProgram + "," + visitTime + "," + width + "," + getRate + "," + staticPageRate + "," + statusCode2xxRate;
    	output.collect(new Text(time), new Text(attributes));
    }
}