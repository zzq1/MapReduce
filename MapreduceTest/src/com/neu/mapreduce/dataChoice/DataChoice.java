package com.neu.mapreduce.dataChoice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.util.FanData;

public class DataChoice {
	public static class DataChoiceMapper extends Mapper<Object, Text, Text, Text> {
		
		private FanData fanData = new FanData();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			fanData.getInstance(value.toString());
			double windspeed = Double.parseDouble(fanData.getWindSpeed());
			if(windspeed > 4 && windspeed <12) {
				context.write(new Text(fanData.getFanNo()), value);
			}else {
				return;
			}
		}
	}

	public static class DataChoiceReducer extends Reducer<Text, Text, Text, NullWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text text : values){
				context.write(text, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path outPut = new Path("/zzq/output/output2/");
		if (fs.exists(outPut)) {
			fs.delete(outPut,true);
		}
//		HDFSUtils hdfs = new HDFSUtils(conf);
//		hdfs.deleteDir(args[1]);
		Job job = Job.getInstance(conf, "Data choice");
		job.setJarByClass(DataChoice.class);
		job.setMapperClass(DataChoiceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(DataChoiceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job, "/zzq/input/2015.csv");
		FileOutputFormat.setOutputPath(job, outPut);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
