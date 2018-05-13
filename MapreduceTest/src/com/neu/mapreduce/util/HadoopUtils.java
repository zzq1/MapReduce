package com.neu.mapreduce.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class HadoopUtils {
	
	/**
     * 分隔符类型,使用正则表达式,表示分隔符为\t或者,
     * 使用方法为SPARATOR.split(字符串)
     */
    public static final Pattern SPARATOR = Pattern.compile("[\t,]");
	
	/**
     * 计算unixtime两两之间的时间差
     *
     * @param timeSortMap key为unixtime,value为pos
     * @return key为pos, value为该pos的停留时间
     */
    public static HashMap<String, Float> calcStayTime(TreeMap<Long, String> timeSortMap) {
        HashMap<String, Float> resMap = new HashMap<String, Float>();
        Iterator<Long> iter = timeSortMap.keySet().iterator();
        Long currentTimeflag = iter.next();
        //遍历treemap
        while (iter.hasNext()) {
            Long nextTimeflag = iter.next();
            float diff = (nextTimeflag - currentTimeflag) / 60.0f;
            //超过60分钟过滤不计
            if (diff <= 60.0) {
                String currentPos = timeSortMap.get(currentTimeflag);
                if (resMap.containsKey(currentPos)) {
                    resMap.put(currentPos, resMap.get(currentPos) + diff);
                } else {
                    resMap.put(currentPos, diff);
                }
            }
            currentTimeflag = nextTimeflag;
        }
        return resMap;
    }
}
