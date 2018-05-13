package com.neu.mapreduce.avgWs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.neu.mapreduce.util.FanData;

public class AvgWs {
	public static class Map extends Mapper<Object,Text,Text,DoubleWritable>{
		private FanData fanData = new FanData();
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			fanData.getInstance(value.toString());
			Double windSpeed = Double.parseDouble(fanData.getWindSpeed());
			context.write(new Text(fanData.getFanNo()), new DoubleWritable(windSpeed));
	        
		}
	}
	public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		private static DoubleWritable result = new DoubleWritable();
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException{
			double sum = 0;
			int count =0;
			for(DoubleWritable s : values) {
				if (s.get()<0)
					continue;
				sum += s.get();
				count ++;
			}
			context.write(new Text("avg"),  new DoubleWritable(sum/count));
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//检查运行命令
//		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
//		if(otherArgs.length != 2){
//			System.err.println("Usage WordCount <int> <out>");
//			System.exit(2);
//		}
		//配置作业名
		Job job = new Job(conf,"word count");
		FileSystem fs = FileSystem.get(conf);  
        Path outPath = new Path("/zzq/output/output3/");  
        if (fs.exists(outPath)) {  
            fs.delete(outPath, true);  
        } 
		//配置作业各个类
		job.setJarByClass(AvgWs.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPaths(job, "/zzq/input/fanData_WT02287.csv");
		FileOutputFormat.setOutputPath(job, outPath);
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
