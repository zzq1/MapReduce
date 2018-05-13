package com.neu.mapreduce.topKGroup;

import java.io.IOException;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.util.FanData;

public class TopKGroup {

	public static class TopKMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private FanData fanData = new FanData();
		private Text word = new Text();
		private static final int K = 10;
		private Map<String, TreeMap<Double, Double>> m = new HashMap<String, TreeMap<Double, Double>>();
		@Override
		protected void map(Object key,Text value, Context context)
				throws IOException, InterruptedException {
			fanData.getInstance(value.toString());
			String month = fanData.getTime().split("/")[0] + "/" + fanData.getTime().split("/")[1];
			String fanNo = fanData.getFanNo();
			String tf = month + " " + fanNo;
			double windSpeed = Double.parseDouble(fanData.getWindSpeed());
			TreeMap<Double, Double> tm = m.get(tf);
			if (tm == null) {
				tm = new TreeMap<Double,Double>();
			}
			tm.put(windSpeed, windSpeed);
			if(tm.size() > K) {
				tm.remove(tm.firstKey());
			}
			m.put(tf, tm);		
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for (String s: m.keySet()) {
				for(Double ws : m.get(s).values()) {
					word.set(s);
					context.write(word, new DoubleWritable(ws));
				}
			}
			
		}
	}
	
	public static class TopKReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private static final int K = 10;
		private Sort sort = new Sort();
		private Map<String, TreeMap<Double, Double>> m = new HashMap<String, TreeMap<Double, Double>>();
		protected void reduce(Text key,Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException { 
			TreeMap<Double, Double> tm = m.get(key.toString());
			if (tm == null) {
				tm = new TreeMap<Double,Double>();
			}
			for(DoubleWritable ws : values) {
				tm.put(ws.get(), ws.get());
				if(tm.size() > K) {
					tm.remove(tm.firstKey());
				}
				m.put(key.toString(), tm);
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			TreeMap<String, TreeMap<Double,Double>> tm = new TreeMap<String, TreeMap<Double,Double>>(m);
			for (String s: tm.keySet()) {
				for(Double ws : m.get(s).values()) {
					context.write(new Text(s), new DoubleWritable(ws));
				}
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path outPut = new Path("/zzq/output/output3/");
		if (fs.exists(outPut)) {
			fs.delete(outPut,true);
		}
		Job job = Job.getInstance();
		job.setJarByClass(TopKGroup.class);
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPaths(job, "/zzq/input/2015.csv");
		FileOutputFormat.setOutputPath(job, outPut);
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
}