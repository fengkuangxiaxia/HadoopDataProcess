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

import Combine.CombineMapper;
import Combine.CombineReducer;
import Combine.CombinePkFkComparator;
import Combine.CombineRecordKey;
import FeatureExtraction.FeatureExtractionMapper;
import FeatureExtraction.FeatureExtractionReducer;
import Join.JoinMapper;
import Join.JoinPkFkComparator;
import Join.JoinRecordKey;
import Join.JoinReducer;

public class DataProcess {

	public static void main(String[] args) throws IOException {
		if(args.length != 2){
            System.err.println("Error!");
            System.exit(1);
        }
		
		Path tempath1 = new Path("/combineTempOutputFold");
		Path tempath2 = new Path("/featureExtractionTempOutputFold");
		Path outDir = new Path(args[1]);
		
		//拼包M/P
        JobConf conf1 = new JobConf(DataProcess.class);
        conf1.setJobName("Combine");
        FileInputFormat.addInputPaths(conf1, args[0]);        
        FileOutputFormat.setOutputPath(conf1, tempath1);
        conf1.setMapperClass(CombineMapper.class);
        conf1.setReducerClass(CombineReducer.class);
        conf1.setOutputKeyClass(NullWritable.class);
        conf1.setOutputValueClass(Text.class);
        
        conf1.setMapOutputKeyClass(CombineRecordKey.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setOutputValueGroupingComparator(CombinePkFkComparator.class);
        
        Job job1 = new Job(conf1);
        
        //特征提取M/P
        JobConf conf2 = new JobConf(DataProcess.class);
        conf2.setJobName("FeatureExtraction");
        FileInputFormat.addInputPaths(conf2, tempath1.toString());        
        FileOutputFormat.setOutputPath(conf2, tempath2);
        conf2.setMapperClass(FeatureExtractionMapper.class);
        conf2.setReducerClass(FeatureExtractionReducer.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        
        Job job2 = new Job(conf2);
        
        job2.addDependingJob(job1);
        
        //Session特征拼接到单条记录M/P
        JobConf conf3 = new JobConf(DataProcess.class);
        conf3.setJobName("Join");
        FileInputFormat.addInputPaths(conf3, tempath2.toString());        
        FileOutputFormat.setOutputPath(conf3, outDir);
        conf3.setMapperClass(JoinMapper.class);
        conf3.setReducerClass(JoinReducer.class);
        conf3.setOutputKeyClass(NullWritable.class);
        conf3.setOutputValueClass(Text.class);
        
        conf3.setMapOutputKeyClass(JoinRecordKey.class);
        conf3.setMapOutputValueClass(Text.class);
        conf3.setOutputValueGroupingComparator(JoinPkFkComparator.class);
        
        Job job3 = new Job(conf3);
        
        job3.addDependingJob(job2);
        
        
        JobControl JC = new JobControl("main");
        JC.addJob(job1);
        JC.addJob(job2);
        JC.addJob(job3);
        
        //删除输出文件夹
        FileSystem fstm = FileSystem.get(conf1);		
		fstm.delete(outDir, true);
		fstm.delete(tempath1, true);
		fstm.delete(tempath2, true);
        
		
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
