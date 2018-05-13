package com.neu.mapreduce.baseStation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 * 位置或者网络信息解析类。
 * @author hdfs
 *
 */
public class LineFormater {

	private String imsi;// 移动用户识别码
	
	private String pos;// 位置信息
	
	private String time;// 原始时间信息
	
	private String timeSlot;// 时间段信息。(例如8-10表示8点到10点。)
	
	private Date day;// 转换为Date类型的时间。
	
	private SimpleDateFormat simpleDataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void format(String line, boolean isPos, String date, String[] timeSlots) throws LineException{
		String[] words = line.split("\t");
        //根据不同数据的格式截取需要的信息
        if (isPos) {
            this.imsi = words[0];
            this.pos = words[3];
            this.time = words[4];
        } else {
            this.imsi = words[0];
            this.pos = words[2];
            this.time = words[3];
        }
        //如果不是当前日期的数据,则过滤并统计异常
        if (!this.time.startsWith(date)) {
            throw new LineException("错误的日期!", -1);
        }
        //将字符串的time字段转成时间类型的day字段,如果该时间格式不正确,则过滤并统计异常
        try {
            this.day = this.simpleDataFormat.parse(this.time);
        } catch (ParseException e) {
            throw new LineException("错误的日期格式!", 0);
        }
        
        // 根据time字段计算timeSlots过程
        // 从time中获得当前的小时信息:2016-02-21 03:43:13得出03
        Integer hour = Integer.valueOf(time.split(" ")[1].split(":")[0]);
        // 在数据长度中循环
        for (int i = 0; i < timeSlots.length; i++) {
            if (Integer.parseInt(timeSlots[i]) <= hour) {
                try {
                    // 如果大于当前时间段,则暂时设置为[当前时间]-[当前时段+1],在下个循环中继续判断
                    // 需要考虑到hour大于最大时段的情况,抛出异常进行统计，例如：08-10表示8点到10点。
                    this.timeSlot = timeSlots[i] + "-" + timeSlots[i + 1];
                } catch (Exception ex) {
                    throw new LineException("时间超出最大时段!", 1);
                }
            } else {
                //如果小于当前时段,则必定是[当前时段-1]-[当前时段]
                //如果是一次循环,则应该是从00时段开始。例如：00-08表示00点到8点。
                if (i == 0) {
                    this.timeSlot = "00-" + timeSlots[i];
                } else {
                    this.timeSlot = timeSlots[i - 1] + "-" + timeSlots[i];
                }
                break;
            }
        }
	}
	
	/**
     * 将imsi和timeflag作为key输出
     * 输出key=移动用户标识,时间段。例如：0000000002,8-10
     * */
    public Text outKey() {
        return new Text(this.imsi + "," + this.timeSlot);
    }

    /**
     * 将pos和day()
     * 输出value=位置,时间(秒)。例如：00000089,1456062937
     * */
    public Text outValue() {
        return new Text(this.pos + "," + this.day.getTime() / 1000L);
    }
	
}
