package com.bourne.HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class TestHiveUDF extends UDF{
	
	public Text evaluate (Text input){
		
		if (input == null){
			return null;
		}
		return new Text(input.toString().toUpperCase());
		
	}

}
