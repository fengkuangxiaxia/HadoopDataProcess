package Combine;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class CombineMapper extends MapReduceBase implements Mapper<LongWritable, Text, CombineRecordKey, Text> {

	
    @Override
    
    public void map(LongWritable key, Text value, 
                      OutputCollector<CombineRecordKey, Text> output, 
                      Reporter reporter) throws IOException {
    	
    	String line = value.toString();
    	String[] elements = line.split("\\|", -1);
        if(elements.length == 12 || elements.length == 11){
            if(elements[1].toUpperCase().equals("G") || elements[1].toUpperCase().equals("P")){
            	String ServerIP = elements[7];
            	String ServerPort = elements[8];
            	String ClientIP = elements[9];
            	String ClientPort = elements[10];
            	
            	CombineRecordKey recoKey = new CombineRecordKey();
        		recoKey.keyId = ServerIP + "|" + ServerPort + "|" + ClientIP + "|" + ClientPort;
        		recoKey.time = elements[0];
        		recoKey.isPrimary = true;
        		output.collect(recoKey, new Text(value.toString() + "\t" + "REQ"));
            } 
            else {
            	String ServerIP = elements[7];
            	String ServerPort = elements[8];
            	String ClientIP = elements[9];
            	String ClientPort = elements[10];
            	
            	CombineRecordKey recoKey = new CombineRecordKey();
        		recoKey.keyId = ClientIP + "|" + ClientPort + "|" + ServerIP + "|" + ServerPort;
        		recoKey.time = elements[0];
        		recoKey.isPrimary = false;
        		output.collect(recoKey, new Text(value.toString() + "\t" + "REP"));
            }
    	}
	    else {
	    	System.out.println(value.toString());
	    }
    }
}