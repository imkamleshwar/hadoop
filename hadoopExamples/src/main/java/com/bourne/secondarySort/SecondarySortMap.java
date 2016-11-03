package com.bourne.secondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMap extends Mapper<LongWritable, Text, SecondarySortCustomKeys, Text> {
	
	private SecondarySortCustomKeys custKeys = new SecondarySortCustomKeys();
	
	private Text first = new Text();
	private Text second = new Text();
	private Text outputValue = new Text();

	public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		String page = tokens[2];
		String ip = tokens[0];
		
		first.set(page);
		second.set(ip);

		custKeys.setFirst(first);
		custKeys.setSecond(second);

		outputValue.set(ip);
			
		context.write(custKeys, outputValue);
	}
}