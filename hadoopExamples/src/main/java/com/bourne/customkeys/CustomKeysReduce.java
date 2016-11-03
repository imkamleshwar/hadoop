package com.bourne.customkeys;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomKeysReduce extends Reducer<CustomKeys, Text, Text, LongWritable>{
	
	private LongWritable pageViews = new LongWritable();
	
	public void reduce(CustomKeys keys, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		
		long pages = 0;
		Text key = new Text();
		
		for (Text t : value) {
			pages++;
		}
		
		String key1 = keys.getFirst().toString();
		String key2 = keys.getSecond().toString();
		
		String key3 = key1 +key2;
		
		key.set(key3);

		pageViews.set(pages);
		context.write(key, pageViews);
		
	}
}
