package FeatureExtraction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class FeatureExtractionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    
    public void map(LongWritable key, Text value, 
                      OutputCollector<Text, Text> output, 
                      Reporter reporter) throws IOException {
 
    	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String str = value.toString();
        //处理
        str = str.replaceAll("[,]", " ");
        for(int i = 0; i < 20; ++i) {
			   str = str.replaceFirst("[|]",",");
        }
        str = str.replaceAll("'", " ");
		str = str.replaceAll("%", "|");
		String[] strArr = str.split(",");
		
		/*
		time = strArr[0];
    	gp = strArr[1];
    	site = strArr[2];
    	url = strArr[3];
    	Type = strArr[4];
    	size = strArr[5];
    	referer = strArr[6];
    	cip = strArr[7];
    	cport = strArr[8];
    	sip = strArr[9];
    	sport = strArr[10];
    	agent = strArr[11];
    	returnTime = strArr[12];
    	statusCode = strArr[13];
    	unknown1 = strArr[14];
    	unknown2 = strArr[15];
    	returnType = strArr[16];
    	returnSize = strArr[17];
    	unknown3 = strArr[18];
    	returnAgent = strArr[19];
		*/
		
		Date time;
		try {
			time = format.parse(strArr[0]);
		} 
		catch (ParseException e1) {
			time = null;
		}
		
		//depth
		int depth = 0;
		try{
			char url[] = strArr[3].toCharArray();
			for(int i = 0; i < strArr[3].length(); ++i) {
				if(url[i] == '/')
					++depth;
			}
			if(strArr[3].indexOf("://") > 0) {
				--depth;
			}
		}
		catch(Exception e){
			depth = 0;
		}

		//parameterNumber
		int parameterNumber = 0;
		try{
			if(strArr[3].indexOf("?") >= 0) {
				String[] strPara1 = strArr[3].split("&");
				parameterNumber = strPara1.length;
			}
		}
		catch(Exception e){
			parameterNumber = 0;
		}
	
		//if exist agent
		String hasAgent = "0";
		try{
			if(strArr[11] == null){
				hasAgent = "0";
			}else{
				hasAgent = "1";
			}
		}
		catch(Exception e){
			hasAgent = "0";
		}
		
		//判断agent是不是程序
		String[] susAgent = {"Python", "python", "Java", "spider", "curl", "fetcher", "PHP"};
		String isAgentProgram = "0";
		try {
			if (strArr[11] != null) {
				for (int i = 0; i < susAgent.length; i++) {
					if (strArr[11].contains(susAgent[i])) {
						isAgentProgram = "1";
						break;
					}
				}
			}
			else {
				isAgentProgram = "1";
			}
		}
		catch (Exception e) {
			isAgentProgram = "0";
		}
		
		String sessionKey = "";
		try {
			sessionKey = strArr[2] + "," + strArr[7] + "," + strArr[11] + "," + (time.getTime() / 1000 / 60 / 30);
		}
		catch (Exception e) {
			sessionKey = strArr[2] + "," + strArr[7] + "," + " " + "," + (time.getTime() / 1000 / 60 / 30);
		}
		
		str = str + "," + depth + "," + parameterNumber + "," + hasAgent + "," + isAgentProgram;
		output.collect(new Text(sessionKey), new Text(str));
    }
}