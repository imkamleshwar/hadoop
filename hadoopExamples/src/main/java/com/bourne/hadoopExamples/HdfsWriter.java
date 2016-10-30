package com.bourne.hadoopExamples;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsWriter extends Configured implements Tool{

	public int run(String[] arg0) throws Exception {
		
		String inputPath = arg0[0];
		Path outputPath = new Path(arg0[1]);
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		OutputStream os = fs.create(outputPath);
		InputStream is = new BufferedInputStream(new FileInputStream(inputPath));
		IOUtils.copyBytes(is, os, conf);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new HdfsWriter(), args));
	}

}