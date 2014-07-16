package FeatureExtraction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class FeatureExtractionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	@Override
    public void reduce(Text key, Iterator<Text> values, 
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
		
	    int visitTime = 0;
	    int width = 0;
	    int staticPage = 0;
	    int getNumber = 0;
	    int statusCode2xx = 0;

	    ArrayList<String> urlFirst = new ArrayList<String>();
	    try{
	    	 while (values.hasNext()) {
	    		String value = values.next().toString();
	    		output.collect(new Text(key), new Text(value + "\tD"));
	    		visitTime++;
	    		
	    		String[] str0 = value.split(",");
	    		//getRate
	    		try{
	    			if(str0[1].equals("G"))
	    				++getNumber;
	    		}
	    		catch(Exception e){
	    			getNumber = 0;
	    		}
		    
	    		//staticPage
	    		try{
	    			String[] staticPageType = str0[3].split("[.]");
	    			if(staticPageType.length > 1 && (staticPageType[1].equals("htm") || staticPageType[1].equals("html") || staticPageType[1].equals("jpg") || staticPageType[1].equals("png") || staticPageType[1].equals("gif"))){
	    				++staticPage;
	    			}
	    		}
	    		catch(Exception e){
	    			staticPage = 0;
	    		}
	    		//staticPage
	    		try{
	    			String[] str1 = str0[3].split("[/]");
    		
	    			if(str1.length > 1) {
	    				if(str0[2].indexOf("://") > 0) {
	    					str1[1] = str1[2];
	    				} 
	    				if(urlFirst.contains(str1[1]) == false){
	    					urlFirst.add(str1[1]);
	    					++width;
	    				}
	    			}
	    		}
	    		catch(Exception e){
	    			width = 0;
	    		}
	    		//状态码为2xx的百分比
	    		try{
	    			String statusCode = str0[13];
	    			if(statusCode.startsWith("2")) {
	    				statusCode2xx++;
	    			}
	    		}
	    		catch(Exception e){
	    			statusCode2xx = 0;
	    		}
	    	}
	    }
	    catch(Exception e1){
	    	 width = 0;
		     staticPage = 0;
		     getNumber = 0;
	    }
	    double getRate = (double)getNumber / (double)visitTime;
	    double staticPageRate = (double)staticPage / (double)visitTime;
	    double statusCode2xxRate = (double)statusCode2xx / (double)visitTime;
		
	    output.collect(new Text(key), new Text(visitTime + "," + width + "," + getRate + "," + staticPageRate + "," + statusCode2xxRate + "\tS"));
 		
    }
}
