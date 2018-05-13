package com.neu.mapreduce.baseStation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BaseStationMapper extends Mapper<Object, Text, Text, Text>{

	private boolean isPosFile;// 是否为pos.txt文件。\
	
	private String date;// 限定日期
	
	private String[] timeSlots;//规定要统计的时间段。
	
	private LineFormater lineFormat = new LineFormater();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		FileSplit fp = (FileSplit)context.getInputSplit();
		String fileName = fp.getPath().getName();// 输入分片所在的文件名称。
		if(fileName.contains("pos")) {
			this.isPosFile = true;
		}else {
			isPosFile = false;
		}
		// yyyy-MM-dd
		this.date = context.getConfiguration().get("date");// 输入参数：日期。
		// 6-12-18
		this.timeSlots = context.getConfiguration().get("timeSlots").split("-");// 输入参数：时间段。
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			// key=移动用户标识,时间段
			// value=位置,时间(秒)
			this.lineFormat.format(value.toString(), this.isPosFile, date, timeSlots);
			// key=移动用户标识,时间段
			// value=位置,时间
			context.write(this.lineFormat.outKey(), this.lineFormat.outValue());
		} catch (LineException e) {
			if (e.getFlag() == -1) {
                context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
            } else if (e.getFlag() == 0) {
                context.getCounter(Counter.TIMESKIP).increment(1);
            } else {
                context.getCounter(Counter.OUTOFTIMEFLASGSKIP).increment(1);
            }
		}catch (Exception e) {
			context.getCounter(Counter.LINESKIP).increment(1);
		}
	}

}
