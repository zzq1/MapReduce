package com.neu.mapreduce.topKGroup;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import java.util.Map.Entry;

public class Sort {
	public Sort() {}
	public String[]  sort_bykey(TreeMap<String,TreeMap<Double,Double>> tm) {
		String[] arr = new String[tm.size()];
		Set<Entry<String, TreeMap<Double, Double>>> entry = tm.entrySet();
		for(Iterator<Entry<String, TreeMap<Double, Double>>> it = entry.iterator();it.hasNext();) {
			Entry<String, TreeMap<Double, Double>> e = it.next();
			for (int i=0;i<tm.size();i++) {
				arr[i] = e.getKey();
			}
		}
		return arr;
	}
}
