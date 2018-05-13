package com.neu.mapreduce.pageRank;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.mapreduce.util.HDFSUtils;
import com.neu.mapreduce.util.HadoopUtils;


/**
 * 将邻接概率矩阵和pr矩阵进行计算并将得到的pr结果输出
 */
public class CalcPageRank {

    /**
     * 输入邻接概率矩阵和pr矩阵
     * 按照矩阵相乘的公式,将对应的数据输出到reduce进行计算
     */
    public static class CalcPeopleRankMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        IntWritable k = new IntWritable();
        Text v = new Text();
        String flag;
        String page_rank;
        String matrix;
        int number_count;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            flag = fileSplit.getPath().getName();
            page_rank = context.getConfiguration().get("page_rank_file_name");
            matrix = context.getConfiguration().get("matrix_file_name");
            number_count = context.getConfiguration().getInt("number_count", -1);
            if (page_rank == null)
                throw new InterruptedIOException("people_rank_file_name not found");
            if (matrix == null)
                throw new InterruptedIOException("matrix_file_name not found");
            if (number_count < 0)
                throw new InterruptedIOException("number_count not found");
        }

        /**
         * k的作用是将pr矩阵的列和邻接矩阵的行对应起来
         * 如:pr矩阵的第一列要和邻接矩阵的第一行相乘,所以需要同时输入到reduce中
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (flag.equals(page_rank)) {//处理pr矩阵
                String[] strArr = HadoopUtils.SPARATOR.split(value.toString());
                k.set(Integer.parseInt(strArr[0]));
                for (int i = 1; i <= number_count; i++) {
                    v.set("pr:" + i + "," + strArr[1]);
                    context.write(k, v);
                }
            } else if (flag.equals(matrix)) {//处理邻接概率矩阵
                String[] strArr = HadoopUtils.SPARATOR.split(value.toString());
                for (int i = 1; i < strArr.length; i++) {
                    k.set(i);
                    v.set("matrix:" + strArr[0] + "," + strArr[i]);
                    context.write(k, v);
                }
            }
        }
    }

    /**
     * 每行输入都是两个矩阵相乘中对应的值
     * 如:邻接矩阵的第一行的值和pr矩阵第一列的值
     */
    public static class CalcPeopleRankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        Text v = new Text();
        float d;//阻尼系数
        int number_count;//人员个数

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            number_count = configuration.getInt("number_count", -1);
            d = configuration.getFloat("damping_coefficient", 0.85f);
            if (number_count < 0)
                throw new InterruptedIOException("number_count not found");
        }

        /**
         * key=被指向的页面ID，value=源页面的PR值&PR矩阵值
         */
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //pr统计
            float pr = 0f;
            //存储pr矩阵列的值
            Map<Integer, Float> prMap = new HashMap<Integer, Float>();
            //存储邻接矩阵行的值
            Map<Integer, Float> matrixMap = new HashMap<Integer, Float>();

            //将两个矩阵对应的值存入对应的map中
            for (Text value : values) {
                String valueStr = value.toString();
                String[] kv = HadoopUtils.SPARATOR.split(valueStr.split(":")[1]);
                if (valueStr.startsWith("pr")) {
                    prMap.put(Integer.parseInt(kv[0]), Float.valueOf(kv[1]));
                } else {
                    matrixMap.put(Integer.parseInt(kv[0]), Float.valueOf(kv[1]));
                }
            }
            //根据map中的数据进行计算
            if (matrixMap.size() != 0) {
                for (Map.Entry<Integer, Float> entry : prMap.entrySet()) {
                    pr += entry.getValue() * matrixMap.get(entry.getKey());
                }
            }
            pr = pr * d + (1 - d) / number_count;
            v.set(String.valueOf(pr));
            context.write(key, v);
        }
    }

    public static void run(String pageRank, String matrix, String outPath, int number_count) throws InterruptedException, IOException, ClassNotFoundException {
        //pageRank = pageRank + "txt";
    	Configuration conf = new Configuration();
        HDFSUtils hdfs = new HDFSUtils(conf);
        conf.set("page_rank_file_name", pageRank);
        conf.set("matrix_file_name", matrix);
        conf.setInt("number_count", number_count);
        Job job = Job.getInstance(conf, "CalcPeopleRank");
        hdfs.deleteDir(outPath);
        job.setJarByClass(CalcPageRank.class);
        job.setMapperClass(CalcPeopleRankMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CalcPeopleRankReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/data/pageRank/" + pageRank ));
        
        
        FileInputFormat.addInputPath(job, new Path("/data/pageRank/output/" + matrix));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.waitForCompletion(true);
        hdfs.deleteDir("/data/pageRank/" + pageRank);
        hdfs.rename(outPath + File.separator + "part-r-00000", "/data/pageRank/" + pageRank);
    }

//    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//         CalcPageRank.run("pageRank.txt","part-r-00000","/data/pageRank/output2/",4);
//    }
}
