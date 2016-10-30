package com.bourne.hadoopExamples;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsReader extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {

		Path inputPath = new Path(arg0[0]);
		String localOutput = arg0[1];

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);

		InputStream is = fs.open(inputPath);

		OutputStream os = new BufferedOutputStream(new FileOutputStream(localOutput));

		IOUtils.copyBytes(is, os, conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new HdfsReader(), args));
	}

}