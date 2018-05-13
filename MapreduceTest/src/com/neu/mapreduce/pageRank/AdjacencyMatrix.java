package com.neu.mapreduce.pageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.util.HDFSUtils;
import com.neu.mapreduce.util.HadoopUtils;


/**
 * 将原始数据集转换成邻接表->邻接矩阵->邻接概率矩阵的过程
 */
public class AdjacencyMatrix {

    /**
     * 输出邻接表
     */
    public static class AdjacencyMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = HadoopUtils.SPARATOR.split(value.toString());
            //原始页面id为key,目标页面id为value
            k.set(strArr[0]);
            v.set(strArr[1]);
            context.write(k, v);
        }
    }

    /**
     * 输入邻接表
     * 输出邻接概率矩阵
     * 邻接矩阵*阻尼系数/该用户链出数+概率矩阵=邻接概率矩阵
     */
    public static class AdjacencyReducer extends Reducer<Text, Text, Text, Text> {

        Text v = new Text();
        int number_count;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            number_count = configuration.getInt("number_count", -1);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //构建邻接矩阵
            float[] P = new float[number_count];
            //该用户的链出数
            int out = 0;
            for (Text value : values) {
                //从value中拿到目标用户的id
                int targetUserIndex = Integer.parseInt(value.toString());
                //邻接矩阵中每个目标用户对应的值为1,其余为0
                P[targetUserIndex - 1] = 1;
                out++;
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < number_count; i++) {
                stringBuilder.append(",").append(P[i] / out);
            }
            v.set(stringBuilder.toString().replaceFirst(",", ""));
            context.write(key, v);
        }
    }

    public static void run(String inPath,String outPath,int number_count) throws InterruptedException, IOException, ClassNotFoundException {
//    	System.getenv().put("-Dhdp.version", "2.5.0.0-1245");
        Configuration conf = new Configuration();
        conf.set("hdp.version", "2.5.0.0-1245");
//        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.setInt("number_count", number_count);
        Job job = Job.getInstance(conf, "AdjacencyMatrix");
        HDFSUtils hdfs = new HDFSUtils(conf);
        hdfs.deleteDir(outPath);
        job.setJarByClass(AdjacencyMatrix.class);
        job.setMapperClass(AdjacencyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(AdjacencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
    	System.setProperty("HADOOP_USER_NAME", "root");
        AdjacencyMatrix.run("/data/pageRank/page.txt","/data/pageRank/output", 4);
    }
}
