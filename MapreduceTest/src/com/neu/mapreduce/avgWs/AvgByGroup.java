package com.neu.mapreduce.avgWs;

import java.io.IOException;
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

public class AvgByGroup {
	//继承mapper接口，设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class MapAvgGroup extends Mapper<Object,Text,Text,DoubleWritable>{
		private FanData fanData = new FanData();
		private Map<String,Double> m = new HashMap<String,Double>();
		double sum = 0.0;
		int count = 0;
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{			
			fanData.getInstance(value.toString());		
			String month1 = fanData.getTime().split("/")[0] + "/" + fanData.getTime().split("/")[1];		
			String fanNo = fanData.getFanNo();
			String tf = month1 + " " + fanNo;
			Double windSpeed = Double.parseDouble(fanData.getWindSpeed());
			if (m.get(tf) == null) {
				sum = 0.0;
				count = 0;
			}
			if (windSpeed >=0) {
				sum += windSpeed;
				count ++;
			}
			m.put(tf, sum/count);       
		}		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for(String ws : m.keySet()) {
				context.write(new Text(ws), new DoubleWritable(m.get(ws)));
			}
		}
	}
	public static class ReduceAvgGroup extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		private Map<String,Double> m = new HashMap<String,Double>();
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException{
			double sum = 0.0;
			int count =0;			
			for(DoubleWritable s : values) {
				if (m.get(key.toString()) == null) {
					sum = 0.0;
					count = 0;
				}
				sum += s.get();
				count ++;
				m.put(key.toString(), sum/count);					  
			}			
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			for(String ws : m.keySet()) {
				context.write(new Text(ws), new DoubleWritable(m.get(ws)));
			}
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
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"word count");
		FileSystem fs = FileSystem.get(conf);  
        Path outPath = new Path("/zzq/output/output3/");  
        if (fs.exists(outPath)) {  
            fs.delete(outPath, true);  
        } 
		//配置作业各个类
		job.setJarByClass(AvgByGroup.class);
		job.setMapperClass(MapAvgGroup.class);
		job.setCombinerClass(ReduceAvgGroup.class);
		job.setReducerClass(ReduceAvgGroup.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPaths(job, "/zzq/input/2015.csv");
		FileOutputFormat.setOutputPath(job, outPath);
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}