package com.topK;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopK {

	public static class TopKMapper extends Mapper<Object, Text, NullWritable, DoubleWritable> {

		private FanData fanData = new FanData();
		
		private static final int K = 10;
		
		private TreeMap<Double, Double> tm = new TreeMap<Double, Double>();
//		private Map<String, TreeMap<Double,Double>> tm = new HashMap<String, TreeMap<Double,Double>>();
		
		@Override
		protected void map(Object key,Text value, Context context)
				throws IOException, InterruptedException {
			fanData.getInstance(value.toString());
			double windSpeed = Double.parseDouble(fanData.getWindSpeed());
			tm.put(windSpeed, windSpeed);
			if(tm.size() > K) {
				tm.remove(tm.firstKey());
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for(Double ws : tm.values()) {
				context.write(NullWritable.get(), new DoubleWritable(ws));
			}
		}
		
	}
	
	public static class TopKReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {

		private static final int K = 10;
		
		private TreeMap<Double, Double> tm = new TreeMap<Double, Double>();
		
		@Override
		protected void reduce(NullWritable key,Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			for(DoubleWritable ws : values) {
				tm.put(ws.get(), ws.get());
				if(tm.size() > K) {
					tm.remove(tm.firstKey());
				}
			}
			for(Double ws : tm.values()) {
				context.write(NullWritable.get(), new DoubleWritable(ws));
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
//		conf.addResource("core-site.xml");
//		conf.addResource("hdfs-site.xml");
		HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(args[1]);
		Job job = Job.getInstance();
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
}
