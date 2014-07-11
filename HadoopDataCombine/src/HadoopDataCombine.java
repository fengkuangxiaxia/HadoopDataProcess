import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class HadoopDataCombine {

	public static void main(String[] args) throws IOException {
		if(args.length != 2){
            System.err.println("Error!");
            System.exit(1);
        }
		
        JobConf conf1 = new JobConf(HadoopDataCombine.class);
        conf1.setJobName("CombineProcess");
        FileInputFormat.addInputPaths(conf1, args[0]);        
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        conf1.setMapperClass(HadoopDataCombineMapper.class);
        conf1.setReducerClass(HadoopDataCombineReducer.class);
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(NullWritable.class);
        
        conf1.setMapOutputKeyClass(RecordKey.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setOutputValueGroupingComparator(PkFkComparator.class);
        
        Job job1 = new Job(conf1);
        
        JobControl JC = new JobControl("main");
        JC.addJob(job1);
        
        FileSystem fstm = FileSystem.get(conf1);
		Path outDir = new Path(args[1]);
		fstm.delete(outDir, true);
        
        //JC.run();
		Thread jcThread = new Thread(JC);  
        jcThread.start();  
        while(true){  
            if(JC.allFinished()){  
                System.out.println(JC.getSuccessfulJobs());  
                JC.stop();  
                return;  
            }  
            if(JC.getFailedJobs().size() > 0){  
                System.out.println(JC.getFailedJobs());  
                JC.stop();  
                return;  
            }  
        }  
	}

}
