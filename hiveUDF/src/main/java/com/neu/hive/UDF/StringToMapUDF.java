package com.neu.hive.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDF;
/** 
 * @description 传入参数格式String:Integer1,Integer2...
 * @return Map<String,ArrayList>
 */
public class StringToMapUDF extends UDF {

    public Map<String, ArrayList<Integer>> evaluate(String course_score) {
        Map<String, ArrayList<Integer>> map = new HashMap<String, ArrayList<Integer>>();
        if (course_score == null || course_score.trim() == "") {
            return null;
        }
        ArrayList<Integer> array = new ArrayList<Integer>();
        String[] mapSplit = course_score.split("\\:");
        if (mapSplit.length > 1) {
            String[] arraySplit = mapSplit[1].split("\\,");
            for (String score : arraySplit) {
                array.add(Integer.valueOf(score));
            }
        }
        map.put(mapSplit[0], array);
        return map;
    }

}