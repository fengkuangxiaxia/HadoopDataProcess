import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
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
import ToWekaForm.ToWekaMapper;
import ToWekaForm.ToWekaReducer;

public class DataProcess {

	public static void main(String[] args) throws IOException {
		if(args.length < 2){
            System.err.println("Error!");
            System.exit(1);
        }
		
		String filename = "result";
		if(args.length >= 3) {
			filename = args[2];
		}
		
		Path combineTempOutputFold = new Path("/combineTempOutputFold");
		Path featureExtractionTempOutputFold = new Path("/featureExtractionTempOutputFold");
		Path joinTempOutputFold = new Path("/joinTempOutputFold");
		Path outDir = new Path(args[1]);
		
		//拼包M/P
        JobConf conf1 = new JobConf(DataProcess.class);
        conf1.setJobName("Combine");
        FileInputFormat.addInputPaths(conf1, args[0]);        
        FileOutputFormat.setOutputPath(conf1, combineTempOutputFold);
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
        FileInputFormat.addInputPaths(conf2, combineTempOutputFold.toString());        
        FileOutputFormat.setOutputPath(conf2, featureExtractionTempOutputFold);
        conf2.setMapperClass(FeatureExtractionMapper.class);
        conf2.setReducerClass(FeatureExtractionReducer.class);
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        
        Job job2 = new Job(conf2);
        
        job2.addDependingJob(job1);
        
        //Session特征拼接到单条记录M/P
        JobConf conf3 = new JobConf(DataProcess.class);
        conf3.setJobName("Join");
        FileInputFormat.addInputPaths(conf3, featureExtractionTempOutputFold.toString());        
        FileOutputFormat.setOutputPath(conf3, joinTempOutputFold);
        conf3.setMapperClass(JoinMapper.class);
        conf3.setReducerClass(JoinReducer.class);
        conf3.setOutputKeyClass(NullWritable.class);
        conf3.setOutputValueClass(Text.class);
        
        conf3.setMapOutputKeyClass(JoinRecordKey.class);
        conf3.setMapOutputValueClass(Text.class);
        conf3.setOutputValueGroupingComparator(JoinPkFkComparator.class);
        
        Job job3 = new Job(conf3);
        
        job3.addDependingJob(job2);
        
        //去除无用的特征
        JobConf conf4 = new JobConf(DataProcess.class);
        conf4.setJobName("ToWekaForm");
        FileInputFormat.addInputPaths(conf4, joinTempOutputFold.toString());        
        FileOutputFormat.setOutputPath(conf4, outDir);
        conf4.setMapperClass(ToWekaMapper.class);
        conf4.setReducerClass(ToWekaReducer.class);
        conf4.setOutputKeyClass(NullWritable.class);
        conf4.setOutputValueClass(Text.class);
        
        conf4.setMapOutputKeyClass(Text.class);
        conf4.setMapOutputValueClass(Text.class);
        
        Job job4 = new Job(conf4);
        
        job4.addDependingJob(job3);
        
        JobControl JC = new JobControl("main");
        JC.addJob(job1);
        JC.addJob(job2);
        JC.addJob(job3);
        JC.addJob(job4);
        
        //删除输出文件夹
        FileSystem fstm = FileSystem.get(conf1);		
		fstm.delete(outDir, true);
		fstm.delete(combineTempOutputFold, true);
		fstm.delete(featureExtractionTempOutputFold, true);
		fstm.delete(joinTempOutputFold, true);
        
		Thread jcThread = new Thread(JC);  
        jcThread.start();  
        while(true){  
            if(JC.allFinished()){  
                System.out.println(JC.getSuccessfulJobs());  
                JC.stop();  
                
                fstm.delete(combineTempOutputFold, true);
        		fstm.delete(featureExtractionTempOutputFold, true);
        		fstm.delete(joinTempOutputFold, true);
                
        		try {
	        		fstm.copyToLocalFile(outDir, new Path("/var/www/security/" + filename));
        		}
        		catch (Exception e) {
        			;
        		}
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
