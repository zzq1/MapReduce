package com.neu.mapreduce.kpi.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.kpi.SortKey;
import com.neu.mapreduce.kpi.SortMapper;
import com.neu.mapreduce.util.HDFSUtils;

public class PVCount {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String inputPath = "/lzb/input/access.20120104.log";
		String outputPath = "/zzq/output/pv/";
		
		// 如果输出目录存在，则删除。
		HDFSUtils hdfs =new HDFSUtils(conf);
		hdfs.deleteDir(outputPath);
		
		Job job = Job.getInstance(conf, "pv-count");
		job.setJarByClass(PVCount.class);
		job.setMapperClass(PVMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(PVReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		
		// 对计算结果做排序
		inputPath = outputPath + "part-*";
		outputPath = outputPath + "sort";
		job = Job.getInstance(conf, "pv-count_sort");
		job.setJarByClass(PVCount.class);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(SortKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
}
