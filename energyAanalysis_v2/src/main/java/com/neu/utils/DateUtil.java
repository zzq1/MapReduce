package com.neu.utils;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import scala.Tuple3;

/**
 * 
 * 功能描述：
 * 
 * @author Administrator
 * @Date Jul 19, 2008
 * @Time 9:47:53 AM
 * @version 1.0
 */
public class DateUtil {

	public static final String DATE_TIME_FORMAT_4SS = "yyyy-MM-dd HH:mm:ss";
	
	public static final String DATE_TIME_FORMAT_4MM = "yyyy-MM-dd HH:mm";

	public static final String DATE_FORMAT_4DD = "yyyy-MM-dd";

	public static final String DATE_FORMAT_YYYYMMDD = "yyyy/MM/dd";

	public static final String DATE_TIME_FORMAT_ALL = "yyyyMMddHHmmss";

	public static final String DATE_TIME_FORMAT_YMD = "yyyyMM";

	public static final String DATE_FORMAT_2DD = "yyMMdd";

	public static final String DATE_FORMAT_4YMD = "yyyyMMdd";

	public static final String DATE_FORMAT_HH24 = "yyMMddHH";

	public static final String DATE_FORMAT_4Y = "yyyy";
	
	public static Date date = null;

	public static DateFormat dateFormat = null;

	public static Calendar calendar = null;

	/**
	 * 功能描述：格式化日期
	 * 
	 * @param dateStr
	 *            String 字符型日期
	 * @param format
	 *            String 格式
	 * @return Date 日期
	 */
	public static Date parseDate(String dateStr, String format) {
		try {
			dateFormat = new SimpleDateFormat(format);
			date = (Date) dateFormat.parse(dateStr);
		} catch (Exception e) {
		}
		return date;
	}

	/**
	 * 功能描述：格式化日期
	 * 
	 * @param dateStr
	 *            String 字符型日期：YYYY-MM-DD 格式
	 * @return Date
	 */
	public static Date parseDate(String dateStr) {
		return parseDate(dateStr, "yyyy/MM/dd");
	}

	/**
	 * 功能描述：格式化输出日期
	 * 
	 * @param date
	 *            Date 日期
	 * @param format
	 *            String 格式
	 * @return 返回字符型日期
	 */
	public static String format(Date date, String format) {
		String result = "";
		try {
			if (date != null) {
				dateFormat = new SimpleDateFormat(format);
				result = dateFormat.format(date);
			}
		} catch (Exception e) {
		}
		return result;
	}

	/**
	 * 功能描述：
	 * 
	 * @param date
	 *            Date 日期
	 * @return
	 */
	public static String format(Date date) {
		return format(date, "yyyy/MM/dd");
	}

	/**
	 * 功能描述：返回年份
	 * 
	 * @param date
	 *            Date 日期
	 * @return 返回年份
	 */
	public static int getYear(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.YEAR);
	}

	/**
	 * 功能描述：返回月份
	 * 
	 * @param date
	 *            Date 日期
	 * @return 返回月份
	 */
	public static int getMonth(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MONTH) + 1;
	}

	/**
	 * 功能描述：返回日份
	 * 
	 * @param date
	 *            Date 日期
	 * @return 返回日份
	 */
	public static int getDay(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.DAY_OF_MONTH);
	}

	/**
	 * 功能描述：返回小时
	 * 
	 * @param date
	 *            日期
	 * @return 返回小时
	 */
	public static int getHour(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	/**
	 * 功能描述：返回分钟
	 * 
	 * @param date
	 *            日期
	 * @return 返回分钟
	 */
	public static int getMinute(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MINUTE);
	}

	/**
	 * 返回秒钟
	 * 
	 * @param date
	 *            Date 日期
	 * @return 返回秒钟
	 */
	public static int getSecond(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.SECOND);
	}

	/**
	 * 功能描述：返回毫秒
	 * 
	 * @param date
	 *            日期
	 * @return 返回毫秒
	 */
	public static long getMillis(Date date) {
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getTimeInMillis();
	}

	/**
	 * 功能描述：返回字符型日期
	 * 
	 * @param date
	 *            日期
	 * @return 返回字符型日期 yyyy/MM/dd 格式
	 */
	public static String getDate(Date date) {
		return format(date, "yyyy/MM/dd");
	}

	/**
	 * 功能描述：返回字符型时间
	 * 
	 * @param date
	 *            Date 日期
	 * @return 返回字符型时间 HH:mm:ss 格式
	 */
	public static String getTime(Date date) {
		return format(date, "HH:mm:ss");
	}

	/**
	 * 功能描述：返回字符型日期时间
	 * 
	 * @param date
	 *            Date 日期
	 * @return 返回字符型日期时间 yyyy/MM/dd HH:mm:ss 格式
	 */
	public static String getDateTime(Date date) {
		return format(date, "yyyy/MM/dd HH:mm:ss");
	}

	/**
	 * 功能描述：日期相加
	 * 
	 * @param date
	 *            Date 日期
	 * @param day
	 *            int 天数
	 * @return 返回相加后的日期
	 */
	public static Date addDate(Date date, int day) {
		calendar = Calendar.getInstance();
		long millis = getMillis(date) + ((long) day) * 24 * 3600 * 1000;
		calendar.setTimeInMillis(millis);
		return calendar.getTime();
	}

	/**
	 * 功能描述：日期相减
	 * 
	 * @param date
	 *            Date 日期
	 * @param date1
	 *            Date 日期
	 * @return 返回相减后的日期
	 */
	public static int diffDate(Date date, Date date1) {
		return (int) ((getMillis(date) - getMillis(date1)) / (24 * 3600 * 1000));
	}

	/**
	 * 功能描述：取得指定月份的第一天
	 * 
	 * @param strdate
	 *            String 字符型日期
	 * @return String yyyy-MM-dd 格式
	 */
	public static String getMonthBegin(String strdate) {
		date = parseDate(strdate);
		return format(date, "yyyy-MM") + "-01";
	}

	/**
	 * 功能描述：取得指定月份的最后一天
	 * 
	 * @param strdate
	 *            String 字符型日期
	 * @return String 日期字符串 yyyy-MM-dd格式
	 */
	public static String getMonthEnd(String strdate) {
		date = parseDate(getMonthBegin(strdate));
		calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, 1);
		calendar.add(Calendar.DAY_OF_YEAR, -1);
		return formatDate(calendar.getTime());
	}

	/**
	 * 功能描述：常用的格式化日期
	 * 
	 * @param date
	 *            Date 日期
	 * @return String 日期字符串 yyyy-MM-dd格式
	 */
	public static String formatDate(Date date) {
		return formatDateByFormat(date, "yyyy-MM-dd");
	}

	/**
	 * 功能描述：以指定的格式来格式化日期
	 * 
	 * @param date
	 *            Date 日期
	 * @param format
	 *            String 格式
	 * @return String 日期字符串
	 */
	public static String formatDateByFormat(Date date, String format) {
		String result = "";
		if (date != null) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(format);
				result = sdf.format(date);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * 获取下一月日期
	 * 
	 * @param date
	 * @return
	 */
	public static String nextMonth(String dataStr, String formatStr) {
		Date date = DateUtil.parseDate(dataStr, formatStr);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.MONTH, 1);
		date = cal.getTime();
		return DateUtil.formatDateByFormat(date, formatStr);
	}
	
	/**
	 * 获取下一天日期
	 * 
	 * @param date
	 * @return
	 */
	public static String nextDay(String dataStr, String formatStr) {
		Date date = DateUtil.parseDate(dataStr, formatStr);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, 1);
		date = cal.getTime();
		return DateUtil.formatDateByFormat(date, formatStr);
	}
	
	/**
	 * 获取下一秒
	 * 
	 * @param date
	 * @return
	 */
	public static String nextSecond(String dataStr, String formatStr) {
		Date date = DateUtil.parseDate(dataStr, formatStr);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.SECOND, 1);
		date = cal.getTime();
		return DateUtil.formatDateByFormat(date, formatStr);
	}

	/**
	 * 获取前一天日期
	 * 
	 * @param date
	 * @return
	 */
	public static String previousDay(String dataStr, String formatStr) {
		Date date = DateUtil.parseDate(dataStr, formatStr);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, -1);
		date = cal.getTime();
		return DateUtil.formatDateByFormat(date, formatStr);
	}

	/**
	 * 日期（精确到秒）转字符串
	 * <P>
	 * Function: dateTime2Str
	 * <P>
	 * Description: //返回指定日期字符串
	 * <P>
	 * Others:
	 * 
	 * @author:
	 * @CreateTime: 2012-9-26
	 * @param date
	 * @return String
	 */
	public static String dateTime2Str(Date date) {
		String str = "";
		if (null != date) {
			str = DateUtil.toString(date, DATE_TIME_FORMAT_4SS);
		}
		return str;
	}
	
	/**
	 * 按指定的格式将日期对象转换为字符串
	 * <P>
	 * Function: toString
	 * <P>
	 * Description:
	 * <P>
	 * Others:返回日期字符串
	 * 
	 * @author: peiyy
	 * @CreateTime: 2012-9-26
	 * @param date
	 * @param format
	 * @return String
	 */
	public static String toString(Date date, String format) {
		if (date == null) {
			return null;
		}
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
	
	/**
	 * 计算两个日期之间的天数
	 * 
	 * @param smdate
	 * @param bdate
	 * @return
	 * @throws ParseException
	 */
	public static int daysBetween(String smdate, String bdate, String formatStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
		Calendar cal = Calendar.getInstance();
		long between_days = 0;
		try {
			cal.setTime(sdf.parse(smdate));
			long time1 = cal.getTimeInMillis();
			cal.setTime(sdf.parse(bdate));
			long time2 = cal.getTimeInMillis();
			between_days = (time2 - time1) / (1000 * 3600 * 24);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return Integer.parseInt(String.valueOf(between_days));
	}
	
	/**
	 * 计算两个日期之间天数
	 * 
	 * @param smdate 开始时间
	 * @param bdate 结束时间
	 * @param formatStr 时间格式
	 * 
	 * @return 两个日期之间的天数
	 */
	public static double timesBetween(String smdate, String bdate, String formatStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
		Calendar cal = Calendar.getInstance();
		double between_days = 0;
		try {
			cal.setTime(sdf.parse(smdate));
			double time1 = cal.getTimeInMillis();
			cal.setTime(sdf.parse(bdate));
			double time2 = cal.getTimeInMillis();
			between_days = (time2 - time1) / (1000 * 3600 * 24);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return between_days;
	}
	
	/**
	 * 获取指定时间的前一小时时间
	 * 
	 * @param args
	 * @throws ParseException
	 */
	public static String getLasthour(String str, String formatStr)
			throws ParseException {
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmm");
		// long millionSeconds = sdf.parse(str).getTime();//毫秒
		Date dt = new Date(str);
		long millionSeconds = dt.getTime();
		millionSeconds -= 60 * 60 * 1000;
		Date date = new Date(millionSeconds);
		SimpleDateFormat dateFormat = new SimpleDateFormat(formatStr);
		return dateFormat.format(date);
	}

	/**
	 * 得到指定月的天数
	 * */
	public static int getMonthLastDay(int year, int month) {
		Calendar a = Calendar.getInstance();
		a.set(Calendar.YEAR, year);
		a.set(Calendar.MONTH, month - 1);
		a.set(Calendar.DATE, 1);// 把日期设置为当月第一天
		a.roll(Calendar.DATE, -1);// 日期回滚一天，也就是最后一天
		int maxDate = a.get(Calendar.DATE);
		return maxDate;
	}

	/**
	 * 计算2000/1/1到指定年月的月初的天数
	 * 
	 * @param year
	 *            年
	 * @param startMonth
	 *            月份
	 * @return 2000/1/1日到指定年月的月初的天数
	 */
	public static int getStartDateNum(int year, int startMonth) {
		int referDateNum = 36526;// Year 2000/1/1 00:00:00
		String smdate = "2000/1/1";
		String bdate = year + "/" + startMonth + "/0";
		int betweenDays = daysBetween(smdate, bdate, "yyyy/MM/dd");// 2000/1/1到当月的日数
		return referDateNum + betweenDays + 1;
	}
	
	/**
	 * 计算2000/1/1到指定日期的的天数
	 * 
	 * @param year
	 *            年
	 * @param startMonth
	 *            月份
	 * @return 2000/1/1到指定日期的月初的天数
	 */
	public static int getStartDateNum(int year, int startMonth, int day) {
		int referDateNum = 36526;// Year 2000/1/1 00:00:00
		String smdate = "2000/1/1";
		String bdate = year + "/" + startMonth + "/" + day;
		int betweenDays = daysBetween(smdate, bdate, "yyyy/MM/dd");// 2000/1/1到当月的日数
		return referDateNum + betweenDays;
	}

	/**
	 * 计算0年0月0日到指定年月的月末的天数
	 * 
	 * @param year
	 *            年
	 * @param startMonth
	 *            月份
	 * @return 0年0月0日到指定年月的月末的天数
	 */
	public static double getEndDateNum(int year, int startMonth) {
		int referDateNum = 36526;// Year 2000/1/1 00:00:00
		String smdate = "2000/1/1";
		String bdate = year + "/" + (startMonth) + "/1";
		double betweenDays = daysBetween(smdate, bdate, "yyyy/MM/dd");// 2000/1/1到当月的日数
		double curMonthDays = getMonthLastDay(year, startMonth);// 当月天数
		return referDateNum + betweenDays + curMonthDays;
	}
	
	/**
	 * 计算2000/1/1到指定日期的月末的天数
	 * 
	 * @param year
	 *            年
	 * @param startMonth
	 *            月份
	 * @return 0年0月0日到指定年月的月末的天数
	 */
	public static double getEndDateNum(int year, int startMonth, int day) {
		int referDateNum = 36526;// Year 2000/1/1 00:00:00
		String smdate = "2000/1/1";
		String bdate = year + "/" + (startMonth) + "/" + day;
		double betweenDays = daysBetween(smdate, bdate, "yyyy/MM/dd");// 2000/1/1到当月的日数
		//double curMonthDays = getMonthLastDay(year, startMonth);// 当月天数
		return referDateNum + betweenDays + 1;
	}
	
	/**
	 * 根据天数，得出对应月份
	 * 
	 * @param time
	 * @return 年份月份。例如：20157；201510
	 */
	public static String getMonthByDays(String days) {
		int referDateNum = 36526;
		int time = (int) Double.parseDouble(days) - referDateNum;

		Calendar calendar = Calendar.getInstance();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date d = sdf.parse("2000-01-01");
			calendar.setTime(d);
			calendar.add(Calendar.DATE, time);

			// System.out.println(a.get(Calendar.DATE)+","+(a.get(Calendar.MONTH)+1)+","+a.get(Calendar.YEAR));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * 根据天数判断是否处于同一月份
	 * @param day1 第一个时间
	 * @param day2 要判断是否与第一个时间处于同一月份的时间
	 * @return 返回true 为处于同一月份 ，返回false为不处于同一月份
	 */
	public static boolean inSameMonthOrNot(String day1, String day2) {
		boolean bool = false;
		if (getMonthByDays(day1).equals(getMonthByDays(day2))) {
			bool = true;
		}
		return bool;
	}

	/**
	 * 计算0年0月0日到指定年月的月末的天数
	 * 
	 * @param year
	 *            年
	 * @param startMonth
	 *            月份
	 * @return 0年0月0日到指定年月的月末的天数
	 */
	public static String getTimes(String date) {
		double referDateNum = 36526;// Year 2000/1/1 00:00:00
		String smdate = "2000-1-1 00:00:00";
		double betweenDays = timesBetween(smdate, date, "yyyy-MM-dd HH:mm:ss");// 2000/1/1到当月的日数
		double result =  referDateNum + betweenDays;
		DecimalFormat df = new DecimalFormat("#.00000");
		return df.format(result);
	}
	
	/**
	 * 根据天数，得出对应天
	 * 
	 * @param time
	 * @return 月份中的某天
	 */
	public static int getDayByDays(String days) {
		int referDateNum = 36526;
		int time = (int) Double.parseDouble(days) - referDateNum;

		Calendar calendar = Calendar.getInstance();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date d = sdf.parse("2000-01-01");
			calendar.setTime(d);
			calendar.add(Calendar.DATE, time);

			// System.out.println(a.get(Calendar.DATE)+","+(a.get(Calendar.MONTH)+1)+","+a.get(Calendar.YEAR));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return calendar.get(Calendar.DAY_OF_MONTH);
	}
	
	/**
	 * 根据天数，得出对应天
	 * 
	 * @param time
	 * @return 月份中的某天
	 */
	public static Tuple3<Integer, Integer, Integer> getDateByDays(String days) {
		int referDateNum = 36526;
		int time = (int) Double.parseDouble(days) - referDateNum;

		Calendar calendar = Calendar.getInstance();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date d = sdf.parse("2000-01-01");
			calendar.setTime(d);
			calendar.add(Calendar.DATE, time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Tuple3<Integer, Integer, Integer>(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1, calendar.get(Calendar.DATE));
	}
	
	/**
	 * 将 double型天数转化为日期类型
	 * @param days double 型天数
	 * @return String 型日期
	 */
	public static String getDateByDou (Double days){
	    String date;
        Calendar base = Calendar.getInstance();   
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
        //Delphi的日期类型从1899-12-30开始
        base.set(1899, 11, 30, 0, 0, 0);
        base.add(Calendar.DATE, days.intValue());   
        base.add(Calendar.MILLISECOND,(int)((days % 1) * 24 * 60 * 60 * 1000));   
        date=outFormat.format(base.getTime());  
		return date;
	}
	/**
	 * 对时间分钟为按照四舍五入取整
	 * @param time 时间
	 * @return 例：传入 2015-1-2 23:1:5 返回 2015-01-02 23:00:00
	 */
	public static String roundDate (String time){
		
		String dateArr[] = time.split(" ");
		int year = Integer.parseInt(dateArr[0].split("-")[0]);
		int month = Integer.parseInt(dateArr[0].split("-")[1]);
		int day = Integer.parseInt(dateArr[0].split("-")[2]);
		int hour = Integer.parseInt(dateArr[1].split(":")[0]);
		int min = Integer.parseInt(dateArr[1].split(":")[1]);
		
		int newMin = (min+5)/10*10;
		if (60 == newMin) {
			hour = hour + 1;
			if (24 == hour) {
				day = day + 1;
				hour = 0;
			}
			newMin = 0;
			if (2 == month) {
				if (year%4==0&&year%100!=0||year%400==0) { // 闰年
					if (30 == day) {
						month = month + 1;
						day = 1;
					}
				}else if (29 ==day) { // 非闰年
					month = month + 1;
					day = 1;
				}
			}
			if (1 == month || 3 == month || 5 == month || 7 == month 
					|| 8 == month || 10 == month || 12 == month) {
				if (32 == day) {
					month = month + 1;
					day = 1;
				}
			}
			if (4 == month || 6 == month || 9 == month || 11 == month ) {
				if (31 == day) {
					month = month + 1;
					day = 1;
				}
			}
			if (13 == month) {
				year = year + 1;
				month = 1;
			}
		}
		String newDate = String.valueOf(year) +"-"+ String.valueOf(month) +"-"+ String.valueOf(day) +
				" "+ String.valueOf(hour) +":"+ String.valueOf(newMin) +":00";
		return format(parseDate(newDate,"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm:ss");
	}
	/**
	 * 计算输入年月日为该年的第几天
	 * @param year 年份
	 * @param month 月份
	 * @param day 日
	 * @return 第一天
	 */
	public static int getDaysOfYear (int year, int month, int day){
		
        int count = 0;
        int days = 0;
        for (int i = 1; i < month; i++) {
            switch (i) {
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12:
                days = 31;
                break;
            case 4:
            case 6:
            case 9:
            case 11:
                days = 30;
                break;
            case 2: {
                if ((year % 4 == 0 && year % 1 != 0) || (year % 400 == 0)) {
                    days = 29;
                } else {
                    days = 28;
                }
                break;
            }
            }
            count = count + days;
        }
        count = count + day;
        return count;
    }
	public static void main(String[] args) {
//		int day = getDayByDays("42005.0694");
//		System.out.println(getDateByDou(42005.0694));
//		 double time = Double.parseDouble(getTimes("2015-04-01 05:40:00"));
//		 double time2 = Double.parseDouble(getTimes("2015-04-01 05:50:00"));
//		 System.out.println((time2 - time) * 24*60);
//		 System.out.println(timesBetween("2015-04-01 05:40:00", "2015-04-01 05:50:00", "yyyy-MM-dd HH:mm:ss")*24*60);
//		 Tuple3<Integer, Integer, Integer> result = getDateByDays(getTimes("2015-05-30 05:40:00"));
//		 System.out.println(result._1() + "" + result._2() + "" + result._3());
//		 System.out.println(getMonthByDays("41640"));
//		 System.out.println(inSameMonthOrNot("41922.15278","41922"));
//		 System.out.println(inSameMonthOrNot("41922.15278","41866"));
//		 String month = "20151";
//		 System.out.println(Integer.parseInt(month.substring(4,month.length())));

		// System.out.println(nextDay("2015/09/30 23:59:59","yyyy/MM/dd HH:mm:ss"));
		// System.out.println(daysBetween("2015/01/01 23:59:59",
		// "2015/12/31 23:59:59", "yyyy/MM/dd HH:mm:ss"));
		// System.out.println(format(addDate(parseDate("2015/01/01 23:59:59",
		// "yyyy/MM/dd HH:mm:ss"), 1), "yyyy/MM/dd HH:mm:ss"));
		// System.out.println(previousDay("2015/09/30 23:59:59","yyyy/MM/dd HH:mm:ss"));
		// System.out.println(nextSecond("2015/01/01 00:00:00","yyyy/MM/dd HH:mm:ss"));
		// System.out.println(nextSecond("20151124200639","yyyyMMddHHmmss"));
//		 System.out.println(getMonthLastDay(2015,3));
		// System.out.println(getStartDateNum(2000, 2));
		// System.out.println(daysBetween("1970/1/1", "2000/1/1",
		// "yyyy/MM/dd"));
//		 System.out.println(getStartDateNum(2000, 12));
//		System.out.println(getEndDateNum(2000,12));
//		System.out.println(nextMonth("2015-12-1 00:00:00", "yyyy-MM-dd HH:mm:ss"));
//		Date startDate = DateUtil.parseDate("201501", "yyyyMM");
//		Date endDate = DateUtil.parseDate("201512", "yyyyMM");
//		System.out.println(DateUtil.format(startDate, "yyyy-MM-dd HH:mm:ss"));
//		String endTime = DateUtil.format(endDate, "yyyy-MM-dd HH:mm:ss");
//		System.out.println(DateUtil.nextMonth(endTime, "yyyy-MM-dd HH:mm:ss"));
//		System.out.println(DateUtil.getStartDateNum(2015, 9, 27));
//		System.out.println(DateUtil.getEndDateNum(2015, 9, 27));
		System.out.println(getDaysOfYear(2015, 12,31));
	}

}
