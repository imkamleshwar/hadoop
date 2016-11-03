package com.bourne.secondarySort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<SecondarySortCustomKeys, Writable>{

	@Override
	public int getPartition(SecondarySortCustomKeys keys, Writable value, int numPartitioners) {
		
		return (keys.getFirst().hashCode() & 0x7FFFFFFF) % numPartitioners;
	}
}