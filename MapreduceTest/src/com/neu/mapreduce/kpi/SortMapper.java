package com.neu.mapreduce.kpi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, SortKey, NullWritable>{

	//使用自定义的类型作为key
    SortKey sortKey = new SortKey();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// key=浏览器类型，value=浏览器出现次数
		String[] kpiInfo = value.toString().split("\t");
		sortKey.setKpiKey(kpiInfo[0]);
		sortKey.setKpiValue(Integer.parseInt(kpiInfo[1]));
		context.write(sortKey, NullWritable.get());
	}
}
