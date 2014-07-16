package ToWekaForm;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 



import java.io.IOException;
import java.util.Iterator;

public class ToWekaReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
	@Override
    public void reduce(Text key, Iterator<Text> values, 
                    OutputCollector<NullWritable, Text> output, 
                    Reporter reporter) throws IOException {
		
		try {	
			while (values.hasNext()) {	
				output.collect(NullWritable.get(), new Text(values.next().toString()));
			}
		} 
		catch (Exception e) {
			;
		}
    }
}