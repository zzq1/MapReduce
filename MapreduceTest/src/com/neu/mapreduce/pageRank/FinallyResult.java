package com.neu.mapreduce.pageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.util.HDFSUtils;
import com.neu.mapreduce.util.HadoopUtils;


/**
 * 对pr值进行重计算,每个pr都除以pr总值
 */
public class FinallyResult {

    public static class FinallyResultMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable
            , Text, Text, Text> {

        Text k = new Text("finally");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            context.write(k, value);
        }
    }

    public static class FinallyResultReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder printStr = new StringBuilder();
            float totalPr = 0f;
            List<String> list = new ArrayList<String>();
            for (Text value : values) {
                String valueStr = value.toString();
                list.add(valueStr);

                String[] strArr = HadoopUtils.SPARATOR.split(valueStr);
                totalPr += Float.parseFloat(strArr[1]);

                printStr.append(",").append(valueStr);
            }
            System.out.println(printStr.toString().replace(",", ""));

            for (int i = (list.size() - 1); i >= 0; i--) {
                String s=list.get(i);
                String[] strArr = HadoopUtils.SPARATOR.split(s);
                k.set(strArr[0]);
                v.set(String.valueOf(Float.parseFloat(strArr[1]) / totalPr));
                context.write(k, v);
            }
        }
    }

    public static void run(String inPath, String outPath) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FinallyResult");
        HDFSUtils hdfs = new HDFSUtils(conf);
        hdfs.deleteDir(outPath);
        job.setJarByClass(FinallyResult.class);
        job.setMapperClass(FinallyResultMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(FinallyResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }

//    public static void main(String[] args) throws Exception {
//        //FinallyResult.run();
//    }
}
