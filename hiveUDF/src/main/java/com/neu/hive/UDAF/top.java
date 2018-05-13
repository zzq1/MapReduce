package com.neu.hive.UDAF;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

@Description(name = "ToUpperCaseUDF", value = "Returns upper case of a given string", extended = "SELECT toUpperCase('Hello World!');")
public final class top extends UDAF{
	
        public static class UDAFTopKState {
            private ArrayList<Double> topk;
        }
        public static class UDAFTopKEvaluator implements UDAFEvaluator{
        	UDAFTopKState state;
        	public UDAFTopKEvaluator() {
        	    super();
        	    state = new UDAFTopKState();
        	    init();
        	}       	
        	public void init(){
        		state.topk=new ArrayList<Double>();
        	}
        	public boolean iterate(Double value) throws HiveException{
        		TreeMap<Double,Double> tMap = new TreeMap<Double,Double>();
        		if(value!=null){
        			tMap.put(value, value);
        			if (tMap.size() > 10) {
						tMap.remove(tMap.firstKey());
					}
        			for(Double wsDouble : tMap.values()) {
        				state.topk.add(tMap.get(wsDouble));
        			}       			
				}
        		return true;   		
        	}
        	public boolean merge(UDAFTopKState o)throws HiveException{
        		TreeMap<Double,Double> tMap1 = new TreeMap<Double,Double>();
        		if(o!=null){
        			state.topk.addAll(o.topk);
        			for(Double s:state.topk) {
        				tMap1.put(s, s);
        				if (tMap1.size() > 10) {
							tMap1.remove(tMap1.firstKey());
						}
        			} 			
        		}
        		for(Double wDouble : tMap1.values()) {
    				state.topk.add(tMap1.get(wDouble));
    			}      		
        		return true;
        	}      	
        	public UDAFTopKState terminatePartial() {
        	    if (state.topk.size() > 0) {
        		return state;
        	    } else {
        		return null;
        	    }
        	}
        	public ArrayList<Double> terminate() {
        		Collections.reverse(state.topk);
        		return state.topk;
        	}       	
        }
}