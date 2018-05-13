package com.neu.hive.UDF;


import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;

public class find_in_columns extends UDF{
    public boolean evaluate(ArrayList<String> column ,String keywords){
        if (column == null || keywords == null) {
        	return false;
        } else {
        	return column.contains(keywords); 
        }
    }
}