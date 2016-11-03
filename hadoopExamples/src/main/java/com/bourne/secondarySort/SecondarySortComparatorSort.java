package com.bourne.secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparatorSort extends WritableComparator {
	
	public SecondarySortComparatorSort(){
		super(SecondarySortCustomKeys.class, true);
	}
	public int compare(WritableComparable a, WritableComparable b) {
		
		SecondarySortCustomKeys lhs = (SecondarySortCustomKeys)a;
		SecondarySortCustomKeys rhs = (SecondarySortCustomKeys)b;
		
		int cmp = lhs.getFirst().compareTo(rhs.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return lhs.getSecond().compareTo(rhs.getSecond());
	}
}