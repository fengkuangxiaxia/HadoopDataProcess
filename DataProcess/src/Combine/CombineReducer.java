package Combine;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CombineReducer extends MapReduceBase implements Reducer<CombineRecordKey, Text, NullWritable, Text> {
	@Override
    public void reduce(CombineRecordKey key, Iterator<Text> values, 
                    OutputCollector<NullWritable, Text> output, 
                    Reporter reporter) throws IOException {
		
		List<String> Request = new ArrayList<String>();
		
		while (values.hasNext()) {	
			String[] tmp = values.next().toString().split("\t");
			try{
				if (tmp[1].equals("REQ")) {
					Request.add(tmp[0]);
				}
				else {
            		String _temp = Request.get(0);
            		String[] elements = tmp[0].split("\\|");
                    Request.remove(0);
                    
                    StringBuffer sb = new StringBuffer();
                    sb.append(_temp + "|");
                    for(int j = 0; j < elements.length; j++){
                    	if(j <= 10 && j >= 7){
                    		continue;
                    	} 
                    	else if(j == (elements.length - 1)){
                    		sb.append(elements[j]);
                    	}
                    	else{
                    		sb.append(elements[j]).append('|');
                    	}
                    }
                    
					output.collect(NullWritable.get(), new Text(new String(sb))); 
				}
			}
			catch (Exception e) {
				Request = new ArrayList<String>();
			}			
		}
		while (!Request.isEmpty()) {
			String _temp = Request.get(0);
            Request.remove(0);
            output.collect(NullWritable.get(), new Text(new String(_temp + "||||||||"))); 
		}
    }
}
