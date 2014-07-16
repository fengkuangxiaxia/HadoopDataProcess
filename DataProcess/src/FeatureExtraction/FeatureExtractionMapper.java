package FeatureExtraction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class FeatureExtractionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    
    public void map(LongWritable key, Text value, 
                      OutputCollector<Text, Text> output, 
                      Reporter reporter) throws IOException {
 
        String str = value.toString();
        //处理
        str = str.replaceAll("[,]", " ");
        for(int i = 0; i < 20; ++i) {
			   str = str.replaceFirst("[|]",",");
        }
        str = str.replaceAll("'", " ");
		str = str.replaceAll("%", "|");
		String[] strArr = str.split(",");
		
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
			sessionKey = strArr[2] + "," + strArr[7] + "," + strArr[11];
		}
		catch (Exception e) {
			sessionKey = strArr[2] + "," + strArr[7] + "," + " ";
		}
		
		str = str + "," + depth + "," + parameterNumber + "," + hasAgent + "," + isAgentProgram;
		output.collect(new Text(sessionKey), new Text(str));
    }
}