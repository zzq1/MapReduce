package com.neu.hive.UDF;


import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class array_contact extends UDF{
    public String evaluate(ArrayList<String> column ,String keywords){
    	if(column.contains(keywords)){
    		for(int j = 0;j  <column.size();) {
    			return column.get(j);
    		}
    	} else {
    		return null;
    	}
		return keywords;
        
    }
}