package com.bourne.secondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReduce extends Reducer<SecondarySortCustomKeys, Text, Text, LongWritable> {
	
	private LongWritable pageViews = new LongWritable();
	
	public void reduce(SecondarySortCustomKeys keys, Iterable<Text> value, Context context) throws IOException, InterruptedException{

		String lastIp = null;
		long pages = 0;
		
		for (Text t : value) {
			String ip = t.toString();
			
			if (lastIp == null) {
				lastIp = ip;
				pages++;
			}
			else if (!lastIp.equals(ip)) {
				lastIp = ip;
				pages++;
			}
			else if (lastIp.compareTo(ip) > 0) {
				throw new IOException("Secondary Sort failed");
			}
		}
		
		pageViews.set(pages);
		context.write(keys.getFirst(), pageViews);		
	}
}		