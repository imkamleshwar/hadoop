package com.bourne.customkeys;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CustomKeys implements WritableComparable<CustomKeys>{
	private Text first;
	private Text second;

	// Implementing no argument Constructor
	public CustomKeys() {
		this.first = new Text();
		this.second = new Text();
	}

	// Implementing Two argument Constructor
	public CustomKeys(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public void readFields(DataInput din) throws IOException {
		first.readFields(din);
		second.readFields(din);
	}

	public void write(DataOutput dout) throws IOException {
		first.write(dout);
		second.write(dout);
	}
	
	public int compareTo(CustomKeys o) {
		CustomKeys other = o;
		int cmp = first.compareTo(other.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(other.getSecond());
	}
	
	public boolean equals(Object obj) {
		if (obj instanceof CustomKeys){
			CustomKeys other = (CustomKeys) obj;
			return first.equals(other.first) && second.equals(other.second);
		}
		return false;
	}
	
	public int hashCode() {
		return first.hashCode();
	}
	
	// Implementing Getter and Setter
	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
}