import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
 



import java.io.IOException;
import java.util.Iterator;

public class HadoopDataProcessReducer extends MapReduceBase implements Reducer<RecordKey, Text, NullWritable, Text> {
	@Override
    public void reduce(RecordKey key, Iterator<Text> values, 
                    OutputCollector<NullWritable, Text> output, 
                    Reporter reporter) throws IOException {
		
		String sessionValue = "";	
		while (values.hasNext()) {	
			String[] tmp = values.next().toString().split("\t");
			if (tmp[1].equals("S")) {
				sessionValue = tmp[0];
			}
			else {
				output.collect(NullWritable.get(), new Text(tmp[0] + "," + sessionValue));
			}
			
		}
		
    }
}

/*
public class HadoopDataProcessReducer extends MapReduceBase implements Reducer<RecordKey, Text, Text, Text> {
	@Override
    public void reduce(RecordKey key, Iterator<Text> values, 
                    OutputCollector<Text, Text> output, 
                    Reporter reporter) throws IOException {
			
		while (values.hasNext()) {	
			output.collect(new Text(key.keyId), new Text(values.next()));			
		}
		
    }
}
*/