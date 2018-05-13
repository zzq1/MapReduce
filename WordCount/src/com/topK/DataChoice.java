package com.topK;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


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
		HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(args[1]);
		Job job = Job.getInstance(conf, "Data choice");
		job.setJarByClass(DataChoice.class);
		job.setMapperClass(DataChoiceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(DataChoiceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
