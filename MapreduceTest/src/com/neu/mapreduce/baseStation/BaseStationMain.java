package com.neu.mapreduce.baseStation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.util.HDFSUtils;

public class BaseStationMain {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
            System.err.println("");
            System.err.println("Usage: Posnet <date> <timepoint> <isTotal>");
            System.err.println("Example: Posnet 2016-02-21 09-18-24");
            System.exit(-1);
        }
		
		Configuration conf = new Configuration();
		conf.set("date", args[0]);
		conf.set("timeSlots", args[1]);//09-14-18
//		String inputPath = "/data/baseStation/*";
		String outputPath = "/zzq/output/baseStation/";
		
		// 判断输出目录是否存在，存在则删除
		HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(outputPath);
		
		Job job = Job.getInstance(conf, "baseStation stay cal");
		job.setJarByClass(BaseStationMain.class);
		job.setMapperClass(BaseStationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(BaseStationReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(4);
		FileInputFormat.addInputPath(job, new Path("/zzq/input/net.txt"));
		FileInputFormat.addInputPath(job, new Path("/zzq/input/pos.txt"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
