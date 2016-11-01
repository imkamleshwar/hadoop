package com.bourne.customkeys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CustomKeysDriver extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(getClass());
		
		job.setMapperClass(CustomKeysMap.class);
		job.setReducerClass(CustomKeysReduce.class);
		
		job.setMapOutputKeyClass(CustomKeys.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
				
		return job.waitForCompletion(true)? 0 : 1;
	}
public static void main(String args[]) throws Exception {
	System.exit(ToolRunner.run(new CustomKeysDriver(), args));
}
	
}