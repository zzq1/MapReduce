package com.topK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import com.neu.mapreduce.util.BasicInfo;
//import com.neu.mapreduce.util.FanData;
//import com.neu.mapreduce.util.HDFSUtils;

public class TableJoin {

	public static class TableJoinMapper extends Mapper<Object, Text, Text, Text> {
		
		private FanData fanData = new FanData();
		
		private BasicInfo bi = new BasicInfo();
		
		private Text text = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			InputSplit inputSplit = context.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName();
			if(fileName.contains("fanData")) {
				fanData.getInstance(value.toString());
				String fanNo = fanData.getFanNo();
				text.set(value.toString() + "_fandata");
				context.write(new Text(fanNo), text);
			}else if (fileName.contains("taizhang")) {
				bi.getInfo(value.toString());
				String fanNo = bi.getFanNo();
				text.set(value.toString() + "_bi");
				context.write(new Text(fanNo), text);
			}
		}
	}
	
	public static class TableJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private BasicInfo bi = new BasicInfo();

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			List<String> fanDatas = new ArrayList<String>();
			String row = null;
			for(Text value : values) {
				row = value.toString();
				if(row.contains("fandata")) {
					fanDatas.add(row);
				}else {
					bi.getInfo(value.toString());
				}
			}
			
			for(String fanData : fanDatas) {
				String newRow = fanData + "," + bi.getFarmName() + "," + bi.getArea() + "," + bi.getProject() + "," + bi.getFanType();
				context.write(new Text(newRow), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Table Join");
		HDFSUtils hdfs = new HDFSUtils(conf);
		hdfs.deleteDir(args[2]);
		job.setJarByClass(TableJoin.class);
		job.setMapperClass(TableJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(TableJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
