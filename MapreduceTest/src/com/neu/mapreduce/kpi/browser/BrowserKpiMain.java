package com.neu.mapreduce.kpi.browser;

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

/**
 * 用户使用的浏览器统计
 * @author hdfs
 *
 */
public class BrowserKpiMain {

	public static void main(String[] args) throws Exception{
		String inputPath = "/lzb/input/access.20120104.log";
		String outputPath = "/zzq/output/brpwer/";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Browser Kpi");
		
		HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(outputPath);
		
		job.setJarByClass(BrowserKpiMain.class);
		job.setMapperClass(BrowserMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(BrowserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(3);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		
		inputPath = outputPath + "part-*";
		outputPath = outputPath + "sort";
		job = Job.getInstance(conf, "Browser Kpi sort");
		job.setJarByClass(BrowserKpiMain.class);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(SortKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
}
