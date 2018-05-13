package com.neu.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;

import com.neu.common.Constants;

public class CommonUtils {
	
	private final static Logger logger = Logger.getLogger(CommonUtils.class);

	/**
	 * 连接两个数组
	 * @param array1
	 * @param array2
	 * @return
	 */
	public static Object[] arraysJoin(Object[] array1, Object[] array2){
		List<Object> list = Arrays.asList(array1);
		List<Object> list2 = Arrays.asList(array2);
		list.addAll(list2);
		return list.toArray();
	}
	
	public static String getMonthNameStr(int year, int month) {
		String monthName = null;
		if(month > 12) {
			month = month - 12;
			year = year + 1;
		}
		monthName = year + "年" + month + "月";
		return monthName;
	}
	
	public static String getMonthNameInt(int year, int month) {
		String monthName = null;
		if(month > 12) {
			month = month - 12;
			year = year + 1;
		}
		monthName = String.valueOf(year) + String.valueOf(month);
		return monthName;
	}
	
	public static int getMonthInt(int startYear, String yearMonth) {
		int year = Integer.parseInt(yearMonth.substring(0, 4));
		int month = Integer.parseInt(yearMonth.substring(4));
		if(startYear < year){
			month = (year - startYear) * 12 + month;
		}
		
		return month;
	}
	
	/**
	 * 根据初始参数生成年月
	 * 例如：年：2014 月：18 =20156
	 * 年2014 月：10=201410
	 * @param startMonth 开始月份
	 * @param endMonth 结束月份
	 * @param startYear 开始年
	 * @return 生成年月字符
	 */
	public static List<String> getMonthKey(int startMonth, int endMonth, int startYear){
		List<String> monthKey = new ArrayList<String>();
		if( endMonth > 12){
			int end = 12-startMonth+1;
			for(int i = 0 ;i < end ; i++) {
				String key = Integer.toString(startYear);
				if (startMonth < 10) {
					key = key + "0"+startMonth;
				}else {
					key = key +Integer.toString(startMonth); 
				}
				monthKey.add(key);
				startMonth++;
			}
			int k = 1;
			int end2 = endMonth-startMonth +1-(12-startMonth+1);
			for(int i = 12-startMonth+1 ;i < end2 ; i++) {
				String key = Integer.toString(startYear+1)+Integer.toString(k); 
				monthKey.add(key);
				k++;
			}
		}else{
			int end = endMonth - startMonth +1;
			for(int i = 0 ;i < end ; i++) {
				String key = Integer.toString(startYear);
				if (startMonth < 10) {
					key = key + "0"+startMonth;
				}else {
					key = key +Integer.toString(startMonth); 
				}
				monthKey.add(key);
				startMonth++;
			}
		}
			
		return monthKey;
	}
	
	/**
	 * 生成开始和结束年月之间的所有年月
	 * 例如：年：201401~201405，生成201401;201402;201403;201404;201405;
	 * @param startMonth 开始年月
	 * @param endMonth 结束年月
	 * @return 生成年月字符集合
	 */
	public static List<String> getMonthKey(String startYearMonth, String endYearMonth){
		List<String> monthKey = new ArrayList<String>();
		if(startYearMonth.equals(endYearMonth)) {
			monthKey.add(startYearMonth);
		}else {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
			Calendar startCal = GregorianCalendar.getInstance();
			startCal.set(Calendar.YEAR, Integer.parseInt(startYearMonth.substring(0,4)));
			startCal.set(Calendar.MONTH, Integer.parseInt(startYearMonth.substring(4)) - 1);
			Calendar endCal = GregorianCalendar.getInstance();
			endCal.set(Calendar.YEAR, Integer.parseInt(endYearMonth.substring(0,4)));
			endCal.set(Calendar.MONTH, Integer.parseInt(endYearMonth.substring(4)) - 1);
			while(true) {
				String month = sdf.format(startCal.getTime().getTime());
				monthKey.add(month);
				if(month.equals(endYearMonth)) {
					break;
				}
				startCal.add(Calendar.MONTH, 1);
			}
			//monthKey.add(sdf.format(startCal.getTime()));
		}
		return monthKey;
	}
	
	/**
	 * 获取某天数据对应的十天功率时间段。
	 * 
	 * @param year 年
	 * @param month 月
	 * @param day 日
	 * @return 当天日期的上一个十天段(年月_区间)
	 */
	public static String getTenDayKey(int year, int month, int day){
		String yearMonthSection = "";
		String section = "";
		if(day <= 10) {
			if(month == 1) {
				month = 12;
				year = year - 1;
			}else {
				month = month - 1;
			}
			section = "3";
		}else if (day <= 20) {
			section = "1";
		}else {
			section = "2";
		}
		Date yearMonthDate = DateUtil.parseDate(year + "" + month, "yyyyMM");
		yearMonthSection = DateUtil.format(yearMonthDate, "yyyyMM") + "_" + section;
		return yearMonthSection;
	}
	
	/**
	 * 获取某天数据对应的十天功率时间段。
	 * 
	 * @param time 转换为天的时间。(例如：42248=2015-09-01 0:00:00)
	 * @return 当天日期的上一个十天段（年月_区间）
	 */
	public static String getTenDayKey(String time){
		Tuple3<Integer, Integer, Integer> yearMonthDay = DateUtil.getDateByDays(time);
		int year = yearMonthDay._1();
		int month = yearMonthDay._2();
		int day = yearMonthDay._3();
		String yearMonthSection = CommonUtils.getTenDayKey(year, month, day);
		return yearMonthSection;
	}
	/**
	 * 获取当前时间所处的10天段
	 * @param time 时间
	 * @return 当天日期所处的时间段 例 42252 = 20150905 = 201509_1
	 */
	public static String getToDayKey(String time){

		String yearMonthSection = "";
		String section = "";
		Tuple3<Integer, Integer, Integer> yearMonthDay = DateUtil.getDateByDays(time);
		int day = yearMonthDay._3();
		if(day <= 10) {
			section = "1";
		}else if (day <= 20) {
			section = "2";
		}else {
			section = "3";
		}
		yearMonthSection = DateUtil.getMonthByDays(time) + "_" + section;
		return yearMonthSection;
	}
	// 加载属性文件
	public static Properties getAllProperties(InputStream in) {
		Properties pro = new Properties();
		try {
			pro.load(in);
		} catch (FileNotFoundException e) {
			logger.error(e);
		} catch (IOException e) {
			logger.error(e);
		} finally{
			try {
				in.close();
			} catch (IOException e) {
				
			}
		}
		return pro;
	} 
	
	/**
	 * list深度拷贝
	 * @param srcList 被拷贝的List对象。
	 * @return 深度拷贝后的新list
	 */
	public static List<List<Double>> listCopy(List<List<Double>> srcList) {
		List<List<Double>> destList = new ArrayList<List<Double>>();
		if(srcList == null) {
			return destList;
		}
		for(int i = 0; i < srcList.size(); i++) {
			List<Double> listRow = srcList.get(i);
			List<Double> newList = new ArrayList<Double>();
			newList.addAll(listRow);
			Collections.copy(newList, listRow);
			destList.add(newList);
		}
		return destList;
	}
	
	/**
	 * 实际功率曲线计算均值
	 * @param referBin1 上次迭代的ReferBin
	 * @param referBin2 本次迭代的ReferBin
	 * @return 均值实际功率曲线。
	 */
	public static Map<String, List<List<Double>>> referBinAvg(
			Map<String, List<List<Double>>> referBinMap1, 
			Map<String, List<List<Double>>> referBinMap2) {
		Set<String> keys = referBinMap2.keySet();
		for(String key : keys) {
			if(!referBinMap1.containsKey(key)) {
				continue;
			}
			List<List<Double>> referBin2 = referBinMap2.get(key);
			List<List<Double>> referBin1 = referBinMap1.get(key);
			// 计算功率曲线之间的均值。
			for(int i = 0; i < referBin2.size(); i++) {
				List<Double> referBin2Row = referBin2.get(i);
				double x2 = referBin2Row.get(0);// 风速
				double y2 = referBin2Row.get(1);// 功率
				double u2 = referBin2Row.get(2);// 上限功率
				double d2 = referBin2Row.get(3);// 下限功率
				List<Double> referBin1Row = referBin1.get(i);
				double x1 = referBin1Row.get(0);// 风速
				double y1 = referBin1Row.get(1);// 功率
				double u1 = referBin1Row.get(2);// 上限功率
				double d1 = referBin1Row.get(3);// 下限功率
				referBin2Row.set(0, (x2 + x1)/2);
				referBin2Row.set(1, (y2 + y1)/2);
				referBin2Row.set(2, (u2 + u1)/2);
				referBin2Row.set(3, (d2 + d1)/2);
			}
		}
		return referBinMap2;
	}
	
	/**
	 * 根据风速，功率曲线获取斜率和偏移量。
	 * @param windSpeed 风速
	 * @param referBin 功率曲线。
	 * @return 斜率和偏移量。<斜率，偏移量>
	 */
	public static Tuple2<Double, Double> getSlopOffset(double windSpeed, List<List<Double>> pcBin){
		double k = 0;
		double b = 0;
		if(windSpeed >= pcBin.get(pcBin.size() - 1).get(0)) {
			// 若10min点风速大于GPC的最大风速，则默认使用功率曲线的最后一个点。
			k = pcBin.get(pcBin.size() - 2).get(2);
			b = pcBin.get(pcBin.size() - 2).get(3);
		}else if(windSpeed < pcBin.get(0).get(0)) {
			// 若10min点风速小于功率曲线的最小风速，则
			k = pcBin.get(0).get(2);
			b = pcBin.get(0).get(3);
		}else {
			for (int iBin = 0; iBin < pcBin.size()-1; iBin++) {
				// 查找当月风速所在 功率曲线的区间
				if (pcBin.get(iBin + 1).get(0) > windSpeed && 
						pcBin.get(iBin).get(0) <= windSpeed) {
					// 根据风速找到斜率、偏移量
					k = pcBin.get(iBin).get(2);
					b = pcBin.get(iBin).get(3);
					break;
				}
			}
		}
		return new Tuple2<Double, Double>(k, b);
	}
	
	/**
	 * 限电持续时长判断，找出未达到持续时长的时间点。
	 * 
	 * @param limitTimesS1 第一阶段限电时间点。
	 * @return 未达到持续时长的时间点。
	 */
	public static List<String> limitDuration(List<String> limitTimesS1){
		List<String> limitTimesS2 = new ArrayList<String>();
		int startIndex = 0;
		int continueCount = 0;
		// (5)条件5:限电持续时长大于等于30分钟。
		for(int i = 0; i < limitTimesS1.size() - 1; i++) {
			// key=风场ID_时间
			String[] key1 = limitTimesS1.get(i).split("_");
			String[] key2 = limitTimesS1.get(i + 1).split("_");
			String farmId1 = key1[0];
			String farmId2 = key2[0];
			double time1 = Double.parseDouble(key1[1]);
			double time2 = Double.parseDouble(key2[1]);
			double minBetween = (time2 - time1)*24*60;
			if(minBetween > 9 && minBetween < 11 && farmId1.equals(farmId2)) {
				// 当前时间点和下个时间点的时间差为10分钟时，持续时间数据条数+1
				continueCount++;
			}else{
				// 若当前时间点和下个时间点不连续，且之前的持续时间大于30min,加入限电时间点中。
				if(continueCount < Constants.BROWNOUTS_TIME_INTERVAL) {
					for (int j = startIndex; j < i + 1; j++) {
						limitTimesS2.add(limitTimesS1.get(j)); 
					}
				}
				continueCount = 0;
				startIndex = i + 1;
			}
		}
		return limitTimesS2;
	}
}
