package com.neu.mapreduce.kpi.browser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mapreduce.kpi.Kpi;

public class BrowserMapper extends Mapper<Object, Text, Text, IntWritable> {
	Text browser = new Text();
    IntWritable one = new IntWritable(1);
    @Override
    protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		// 1.解析kpi数据，从log中提取各个字段信息。
		Kpi kpi = Kpi.parse(value.toString());
		// 2.将浏览器信息,1作为map的输出。
		if(kpi.getIs_validate()) {
			browser.set(kpi.getUser_agent());
			context.write(browser, one);
		}
	}
}
