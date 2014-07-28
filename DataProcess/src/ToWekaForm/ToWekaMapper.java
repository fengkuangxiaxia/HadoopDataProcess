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
    	int depth = 0;
    	int parameterNumber = 0;
    	int hasAgent = 0;
    	int isAgentProgram = 0;
    	//session特征
    	int visitTime = 0;
    	int width = 0;
    	double getRate = 0.0;
    	double staticPageRate = 0.0;
    	double statusCode2xxRate = 0.0;
    	
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
	    	depth = Integer.valueOf(temp[20]);
	    	parameterNumber = Integer.valueOf(temp[21]);
	    	hasAgent = Integer.valueOf(temp[22]);
	    	isAgentProgram = Integer.valueOf(temp[23]);
	    	visitTime = Integer.valueOf(temp[24]);
	    	width = Integer.valueOf(temp[25]);
	    	getRate = Double.valueOf(temp[26]);
	    	staticPageRate = Double.valueOf(temp[27]);
	    	statusCode2xxRate = Double.valueOf(temp[28]);
    	}
    	catch (Exception e) {
    		;
    	}

    	//String attributes = depth + "," + parameterNumber + "," + hasAgent + "," + isAgentProgram + "," + visitTime + "," + width + "," + getRate + "," + staticPageRate + "," + statusCode2xxRate;
    	//output.collect(new Text(time), new Text(attributes));
    	if (judge(depth, parameterNumber, hasAgent, isAgentProgram, visitTime, width, getRate, staticPageRate, statusCode2xxRate) == -1) {
    		output.collect(new Text(time), new Text(line));
    	}
    }
    
    private int judge(int depth, int parameterNumber, int hasAgent, int isAgentProgram, int visitTime, int width, double getRate, double staticPageRate, double statusCode2xxRate) {
    	 if (isAgentProgram == 0) {
    		if (visitTime <= 966) {
    			if (visitTime <= 12) {
    				if (depth <= 9) {
    					return 1;
    				}
    				else {
    					if (width <= 1) {
    						if (parameterNumber <= 4) {
    							return -1;
    						}
    						else {
    							return 1;
    						}
    					}
    					else {
    						return 1;
    					}
    				}
    			}
    			else {
    				return 1;
    			}
    		} 
    		else {
    			return -1;
    		}
    	 }
    	 else {
    		 if (visitTime <= 8) {
    			 if (width <= 0) {
    				 return (visitTime <= 1) ? 1 : -1;
    			 }
    			 else {
    				 if (visitTime <= 1) {
    					 if (depth <= 1) {
    						 if (statusCode2xxRate <= 0.5) {
    							 return 1;
    						 }
    						 else {
    							 if (staticPageRate <= 0.5) {
    								 return (parameterNumber <= 0) ? -1 : 1;
    							 }
    							 else {
    								 return 1;
    							 }
    						 }
    					 }
    					 else {
    						 return 1;
    					 }
    				 }
    				 else {
    					 if (statusCode2xxRate <= 0.833333) {
    						 if (staticPageRate <= 0.833333) {
    							 return 1;
    						 }
    						 else {
    							 return (visitTime <= 2) ? 1 : -1;
    						 }
    					 }
    					 else {
    						 if (visitTime <= 5) {
    							 if (depth <= 2) {
    								 if (width <= 2) {
    									 return 1;
    								 }
    								 else {
    									 return (staticPageRate <= 0.375) ? -1 : 1;
    								 }
    							 }
    							 else {
    								 if (depth <= 5) {
    									 if (width <= 1) {
    										 if (visitTime <= 3) {
    											 return (visitTime <= 2) ? -1 : 1;
    										 }
    										 else {
    											 return (staticPageRate <= 0.875) ? 1 : -1;
    										 }
    									 }
    									 else {
    										 if (depth <= 3) {
    											 return 1;
    										 }
    										 else {
    											 if (width <= 2) {
    												 return -1;
    											 }
    											 else {
    												 return (staticPageRate <= 0.375) ? -1 : 1;
    											 }
    										 }
    									 }
    								 }
    								 else {
    									 return (width <= 1) ? 1 : -1;
    								 }
    							 }
    						 }
    						 else {
    							 return -1;
    						 }
    					 }
    				 }
    			 }
    		 }
    		 else {
    			 return -1;
    		 }
    	 }
    }
}