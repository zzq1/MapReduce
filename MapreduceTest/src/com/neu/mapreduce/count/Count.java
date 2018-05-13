package com.neu.mapreduce.count;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.neu.mapreduce.util.FanData;

public class Count {
	public static class Map extends Mapper<Object,Text,Text,IntWritable>{
		private static IntWritable one = new IntWritable(1);
		private FanData fanData = new FanData();
		private Text word = new Text();
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
			fanData.getInstance(value.toString());
			String fanNO = fanData.getFanNo();
			StringTokenizer st = new StringTokenizer(fanNO.toString());
			while(st.hasMoreTokens()){
				word.set(st.nextToken());
				context.write(word, one);
			}
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private static IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
		    Iterator<IntWritable> iterator = values.iterator();
        	while (iterator.hasNext()) {
        		sum += iterator.next().get();
            }
			result.set(sum);
			context.write(new Text("count"),  result);
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
        Path outPath = new Path("/zzq/output/output1/");  
        if (fs.exists(outPath)) {  
            fs.delete(outPath, true);  
        } 
		//配置作业各个类
		job.setJarByClass(Count.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPaths(job, "/zzq/input/fanData_WT02287.csv");
		FileOutputFormat.setOutputPath(job, outPath);
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

