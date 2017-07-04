package MapReduce; 

import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRdriver extends Configured implements Tool {

   public int run(String[] args) throws Exception {
	   
	    
      // TODO: configure first MR job 
	  Job job1 = new Job(getConf(), "Map Reduce Job 1"); 
      job1.setJarByClass(MRdriver.class);
	  // TODO: setup input and output paths for first MR job
	  job1.setMapperClass(MRmapper1.class);
	  job1.setReducerClass(MRreducer1.class);
	  job1.setInputFormatClass(TextInputFormat.class);
	  job1.setOutputKeyClass(Text.class);
	  job1.setOutputValueClass(IntWritable.class);
	  job1.setMapOutputValueClass(IntWritable.class);
	  FileInputFormat.addInputPath(job1, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	  
      // TODO: run first MR job syncronously with verbose output set to true
	  
	  if(!job1.waitForCompletion(true)){
		  return 1;
	  }
	  // TODO: configure the second MR job 
	 
	  Job job2 = new Job(getConf(), "Map Reduce Job 2");
	  job2.setJarByClass(MRdriver.class);
	  
      // TODO: setup input and output paths for second MR job
	  
	  job2.setMapperClass(MRmapper2.class);
	  job2.setReducerClass(MRreducer2.class);
	  job2.setInputFormatClass(TextInputFormat.class);
	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(DoubleWritable.class);
	  job2.setMapOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job2, new Path(args[1]));
	  FileOutputFormat.setOutputPath(job2, new Path(args[2]));
      
	  // TODO: run second MR job syncronously with verbose output set to true
      
	  if(!job2.waitForCompletion(true)){
		  return 1;
	  }
	  
      // TODO: detect anomaly based on sigma_threshold provided by user
	  
	  int sigma_threshold = Integer.parseInt(args[3]);

      // TODO: for each user with score higher than threshold, print to screen:
	  BufferedReader in = null;
	  in = new BufferedReader(new FileReader(args[2]+"/part-r-00000"));
	  
	  in.readLine();in.readLine();
	  String lineRead;
	  while((lineRead=in.readLine())!=null){
	      String[] vals = lineRead.split("\\s+");
	      if(vals.length==2 && Double.parseDouble(vals[1])>sigma_threshold){
	    	  String statement = "detected anomaly for user: "+vals[0].replaceFirst("num_sigmas_for:","")+" with score: "+vals[1];
	    	  System.out.println(statement);
	      }
	 }
      // detected anomaly for user: <username>  with score: <numSigmas>
	  return 0;

   }

   public static void main(String[] args) throws Exception { 
	   
	   if(args.length != 4) {
		   System.err.println("usage: MRdriver <input-path> <output1-path> <output2-path> <sigma_int_threshold>");
		   System.exit(1);
	   }
	   // check sigma_int_threshold is an int
	  try {
		  Integer.parseInt(args[3]);
	  }
	  catch (NumberFormatException e) {
		  System.err.println(e.getMessage());
		  System.exit(1);
	  }
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new MRdriver(), args));
   } 
}
