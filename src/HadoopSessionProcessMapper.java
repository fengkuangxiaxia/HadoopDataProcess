import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class HadoopSessionProcessMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    @Override
    
    public void map(LongWritable key, Text value, 
                      OutputCollector<Text, Text> output, 
                      Reporter reporter) throws IOException {
 
        String str = value.toString();
        //¥¶¿Ì
        str = str.replaceAll("[,]", " ");
        for(int i = 0; i < 11; ++i) {
			   str = str.replaceFirst("[|]",",");
        }
        str = str.replaceAll("'", " ");
		str = str.replaceAll("%", "|");
		String[] strArr = str.split(",");
		
		//length
		int length = 0;
		try{
			char url[] = strArr[3].toCharArray();
			for(int i = 0; i < strArr[3].length(); ++i) {
				if(url[i] == '/')
					++length;
			}
			if(strArr[3].indexOf("://") > 0) {
				--length;
			}
		}
		catch(Exception e){
			length = 0;
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
		short agent = 0;
		try{
			if(strArr[11] == null){
				agent = 0;
			}else{
				agent = 1;
			}
		}
		catch(Exception e){
			agent = 0;
		}
		
		String sessionKey = "";
		try {
			sessionKey = strArr[2] + "," + strArr[7] + "," + strArr[11];
		}
		catch (Exception e) {
			sessionKey = strArr[2] + "," + strArr[7] + "," + " ";
		}
		
		FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
        String filename = fileSplit.getPath().getName();
        String isMal = (filename.indexOf("a") >= 0) ?  "1" : "0";
		
		str = str + "," + isMal + "," + length + "," + parameterNumber + "," + agent;
		output.collect(new Text(sessionKey), new Text(str));
    }
}