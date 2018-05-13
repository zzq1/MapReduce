package com.neu.mapreduce.baseStation;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.mapreduce.util.HadoopUtils;

/**
 * 计算相同用户每个时间段停留时间。
 * map过来的数据格式为：key=移动用户标识,时间段; value=位置,时间
 * 
 * 1.按unixtime从小到大进行排序
 * 2.添加OFF位的unixtime(当前时段的最后时间)
 * 3.从大到小一次相减得到每个位置的停留时间
 * @author hdfs
 *
 */
public class BaseStationReducer extends Reducer<Text, Text, NullWritable, Text>{

	//要计算的时间（yyy-MM-dd）
    String day;
    Text out = new Text();
    
    // 同一用户，按照时间从小到大排序的基站信息。key=时间点，value=基站信息
    private TreeMap<Long, String> timeSortMap = new TreeMap<Long, String>();
    
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		this.day = context.getConfiguration().get("date");
	}

	//key=移动用户标识,时间段（000000999,6-12）
	// value=基站,时间点
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 1.按照时间对用户信息做排序。
		for(Text value : values) {
			String[] info = value.toString().split(",");
			timeSortMap.put(Long.parseLong(info[1]), info[0]);
		}
		
		// 2.加入每个时间段的末尾时间点。
		String[] keyInfo = key.toString().split(",");
		String imsi = keyInfo[0];// 移动用户标识
		String timeSlot = keyInfo[1];// 时间段
		
        try {
        	//设置该数据所在的最后时段的unixtime
			Date offTimeflag = simpleDateFormat.parse(this.day + " " + timeSlot.split("-")[1] + ":00:00");
			timeSortMap.put(offTimeflag.getTime() / 1000L, "OFF");
			
			// 3.计算用户在各个基站的停留时间。
			HashMap<String, Float> resMap = HadoopUtils.calcStayTime(timeSortMap);
			// 4.输出用户在各个基站的停留时间。
            for (Map.Entry<String, Float> entry : resMap.entrySet()) {
                String builder = imsi + "|" +
                        timeSlot + "|" +
                        entry.getKey() + "|" +
                        entry.getValue();
                out.set(builder);
                context.write(NullWritable.get(), out);
            }
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
