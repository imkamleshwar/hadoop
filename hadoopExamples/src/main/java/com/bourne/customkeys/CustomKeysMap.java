package com.bourne.customkeys;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomKeysMap extends Mapper<LongWritable, Text, CustomKeys, Text> {
	
	private CustomKeys custKeys = new CustomKeys();
	
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
		
/*		// TESTING FOR DESERIALISATION 
		Text d1 = new Text();
		Text d2 = new Text();
		d1 = custKeys.getFirst();
		d2 = custKeys.getSecond();
		System.out.println("Printing after deserialization: "+d1+d2);
*/
	
		context.write(custKeys, outputValue);
	}
}