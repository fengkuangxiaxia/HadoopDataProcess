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



public class HadoopDataProcess {

	public static void main(String[] args) throws IOException {
		if(args.length != 2){
            System.err.println("Error!");
            System.exit(1);
        }
 
		Path tempath = new Path("tmp");
		
        JobConf conf1 = new JobConf(HadoopDataProcess.class);
        conf1.setJobName("SessionProcess");
        FileInputFormat.addInputPaths(conf1, args[0]);        
        FileOutputFormat.setOutputPath(conf1, tempath);
        conf1.setMapperClass(HadoopSessionProcessMapper.class);
        conf1.setReducerClass(HadoopSessionProcessReducer.class);
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(Text.class);
        Job job1 = new Job(conf1);
        
        JobConf conf2 = new JobConf(HadoopDataProcess.class);
        conf2.setJobName("DataProcess");
        FileInputFormat.setInputPaths(conf2, new Path("tmp/part-00000"));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
        conf2.setMapperClass(HadoopDataProcessMapper.class);
        conf2.setReducerClass(HadoopDataProcessReducer.class);
        conf2.setOutputKeyClass(NullWritable.class);
        //conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        
        conf2.setMapOutputKeyClass(RecordKey.class);
        conf2.setMapOutputValueClass(Text.class);
        conf2.setOutputValueGroupingComparator(PkFkComparator.class);
        
        Job job2 = new Job(conf2);
        
        job2.addDependingJob(job1);
        
        JobControl JC = new JobControl("main");
        JC.addJob(job1);
        JC.addJob(job2);
        
        FileSystem fstm = FileSystem.get(conf1);
		Path outDir = new Path(args[1]);
		fstm.delete(outDir, true);
		fstm.delete(tempath, true);
        
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
