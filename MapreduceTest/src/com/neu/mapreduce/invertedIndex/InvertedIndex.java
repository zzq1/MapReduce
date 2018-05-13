package com.neu.mapreduce.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 实现简单的倒排索引
 */
public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void map(
                LongWritable key,
                Text value,
                org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            //FileSplit类从context上下文中得到，可以获得当前读取的文件的路径
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            //文件路径为hdfs://hadoop:9000/ii/a.txt
            //根据/分割取最后一块即可得到当前的文件名
            String[] fileNames = fileSplit.getPath().toString().split("/");
            String fileName = fileNames[fileNames.length - 1];
            for (String d : data) {
                k.set(d + "->" + fileName);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            //分割文件名和单词
            String[] wordAndPath = key.toString().split("->");
            //统计出现次数
            int counts = 0;
            for (Text t : values) {
                counts += Integer.parseInt(t.toString());
            }
            //组成新的key-value输出,key=单词->文件名;value=文件名->单词数
            k.set(wordAndPath[0]);
            v.set(wordAndPath[1] + "->" + counts);
            context.write(k, v);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();

        protected void reduce(
                Text key,
                java.lang.Iterable<Text> values,
                org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
                throws java.io.IOException, InterruptedException {
            String res = "";
            for (Text text : values) {
                res += text.toString() + "\r";
            }
            v.set(res);
            context.write(key, v);
        }
    }

    public static void run() throws InterruptedException, IOException, ClassNotFoundException {
    	Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(conf);  
        Path outPath = new Path("/zzq/output/InvertedIndex/");  
        if (fs.exists(outPath)) {  
            fs.delete(outPath, true);  
        }  
        Job job = Job.getInstance(conf);  
        job.setJarByClass(InvertedIndex.class);  
  
        FileInputFormat.addInputPaths(job, "/zzq/input/a.txt");  
        FileInputFormat.addInputPaths(job, "/zzq/input/b.txt");  
        job.setInputFormatClass(TextInputFormat.class);  
  
        job.setMapperClass(InvertedIndexMapper.class);  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(Text.class);  
  
        job.setCombinerClass(InvertedIndexCombiner.class);  
  
        job.setReducerClass(InvertedIndexReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
  
        FileOutputFormat.setOutputPath(job, outPath);  
        job.setOutputFormatClass(TextOutputFormat.class);  
  
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        InvertedIndex.run();
    }
}
