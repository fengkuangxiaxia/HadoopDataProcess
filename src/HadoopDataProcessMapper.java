import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class HadoopDataProcessMapper extends MapReduceBase implements Mapper<LongWritable, Text, RecordKey, Text> {

	
    @Override
    
    public void map(LongWritable key, Text value, 
                      OutputCollector<RecordKey, Text> output, 
                      Reporter reporter) throws IOException {

    	if(value.toString().endsWith("S")) {
    		RecordKey recoKey = new RecordKey();
			recoKey.keyId = value.toString().split("\t")[0];
			recoKey.isPrimary = true;
			output.collect(recoKey, new Text(value.toString().split("\t")[1] + "\t" + value.toString().split("\t")[2]));
    	}
    	else {
    		RecordKey recoKey = new RecordKey();
			recoKey.keyId = value.toString().split("\t")[0];
			recoKey.isPrimary = false;
			output.collect(recoKey, new Text(value.toString().split("\t")[1] + "\t" + value.toString().split("\t")[2]));
    	}
    }
}