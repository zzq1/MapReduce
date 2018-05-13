package com.neu.mapreduce.kpi.ips;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpsReducer extends Reducer<Text, Text, Text, IntWritable>{

	private IntWritable count = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
		Set<String> ips = new HashSet<String>();
		for(Text ip : values) {
			ips.add(ip.toString());
		}
		count.set(ips.size());
		context.write(key, count);
	}
}
