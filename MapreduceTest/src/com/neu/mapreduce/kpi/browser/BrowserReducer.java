package com.neu.mapreduce.kpi.browser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable resCount = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		Integer sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        resCount.set(sum);
        context.write(key, resCount);
	}
}
