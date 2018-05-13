package com.neu.mapreduce.kpi.time;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mapreduce.kpi.Kpi;

public class TimeMapper extends Mapper<Object, Text, Text, IntWritable>{

	private Text time = new Text();
	
	private IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Kpi kpi = Kpi.parse(value.toString());
		if(kpi.getIs_validate()) {
			time.set(kpi.getRequest_time());
			context.write(time, one);
		}
	}
}
