package com.bourne.secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupComparator extends WritableComparator{
	
	public SecondarySortGroupComparator(){
		super(SecondarySortCustomKeys.class, true);
	}
	
	public int compare(WritableComparable a, WritableComparable b){
		
		SecondarySortCustomKeys lhs = (SecondarySortCustomKeys) a;
		SecondarySortCustomKeys rhs = (SecondarySortCustomKeys) b;
		
		return lhs.getFirst().compareTo(rhs.getFirst());		
	}
}
