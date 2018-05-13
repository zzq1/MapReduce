package com.neu.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class MathUtil {

	private static Logger log = Logger.getLogger(MathUtil.class);
	
	/**
	 * 格式化double数据。(四舍五入)
	 * @param value 被格式化double值
	 * @param decimal 保留小数点后位数
	 * @return 格式化后的double值。
	 */
	public static double doubleFormat(Double value, int decimal) {
		double result = 0.00d;
		DecimalFormat df = new DecimalFormat("#.#######");
		if(value != null && !value.isNaN() && !value.isInfinite()) {
			df.setMaximumFractionDigits(decimal);
			df.setRoundingMode(RoundingMode.HALF_UP);
			result = Double.parseDouble(df.format(value));
		}
		return result;
	}
	
	/**
	 * 批量保留数据小数点位数
	 * @param values 数据集合
	 * @param decimal 小数点位数；2表示后两位
	 * @return
	 */
	public static List<Double> roundn(List<Double> values, int decimal) {
		List<Double> list = new ArrayList<Double>();
		BigDecimal bd = null;
		for (Double value : values) {
			if(value != null && !value.isNaN() && !value.isInfinite()) {
				bd = new BigDecimal(value);
				bd = bd.setScale(decimal, BigDecimal.ROUND_HALF_UP);
				list.add(Double.parseDouble(bd.toString()));
			}else {
				list.add(0.0);
			}
		}
		bd = null;
		return list;
	}
	
	/**
	 * 批量保留数据小数点位数
	 * @param values 数据集合
	 * @param decimal 小数点位数；2表示后两位
	 * @return
	 */
	public static List<String> roundnStr(List<Double> values, int decimal) {
		List<String> list = new ArrayList<String>();
		BigDecimal bd = null;
		for (Double value : values) {
			if(value != null && !value.isNaN() && !value.isInfinite()) {
				bd = new BigDecimal(value);
				bd = bd.setScale(decimal, BigDecimal.ROUND_HALF_UP);
				list.add(bd.toString());
			}else {
				list.add("0.0");
			}
		}
		bd = null;
		return list;
	}
	
	/**
	 * 批量保留数据小数点位数
	 * @param values 数据集合
	 * @param decimal 小数点位数；2表示后两位
	 * @return
	 */
	public static List<String> roundn1(List<String> values, int decimal) {
		List<String> list = new ArrayList<String>();
		if(values == null) return list;
		BigDecimal bd = null;
		for (String value : values) {
			if(StringUtils.isNotBlank(value) && !"NaN".equals(value) && !"Infinity".equals(value)) {
				bd = new BigDecimal(value);
				bd = bd.setScale(decimal, BigDecimal.ROUND_HALF_UP);
				list.add(bd.toString());
			}else {
				list.add("0.0");
			}
		}
		bd = null;
		return list;
	}
	
	/**
	 * 按列求和统计
	 * @param sumList
	 * @return
	 */
	public static List<Double> nansum(List<List<Double>> sumList){
		List<Double> resultList = new ArrayList<Double>();
		Double[] arrDouble = null;
		for(int i = 0; i < sumList.size(); i++){
			if(sumList.get(i) != null){
				if(sumList.get(i).size()>0){
					arrDouble = new Double[sumList.get(i).size()];
				}
				break;
			}
		}
		if(arrDouble == null){
			return resultList;
		}
		
		for(int i = 0; i < sumList.size(); i++){// row
			
			List<Double> tempList = sumList.get(i);
			if(tempList == null) continue;
			for(int j = 0; j < tempList.size(); j++){
				Object temp = tempList.get(j);
				Double value = 0.0;
				if(temp instanceof Double) {
					value = (Double)temp;
				}else if(temp instanceof String) {
					value = Double.parseDouble(String.valueOf(temp));
				} 
				arrDouble[j] = (arrDouble[j] == null ? 0:arrDouble[j]) + value;
			}
		}
		Collections.addAll(resultList, arrDouble);
		arrDouble = null;
		
		return resultList;
	}
	
	/**
	 * 按列求和统计
	 * @param sumList
	 * @return
	 */
	public static List<Double> nansumStr(List<List<String>> sumList) {
		if(sumList == null) {
			return new ArrayList<Double>();
		}
		List<List<Double>> list = ConvertUtil.convertStrToDou(sumList);
		return nansum(list);
	}
	
	/**
	 * 按列求和统计
	 * @param sumList
	 * @return
	 */
	public static List<Double> nansumStr(List<List<String>> sumList, int endIndex) {
		if(sumList == null) {
			return new ArrayList<Double>();
		}
		List<List<Double>> list = ConvertUtil.convertStrToDou(sumList, endIndex);
		return nansum(list);
	}
	
	/**
	 * 统计某列和
	 * @param sumList 集合
	 * @param col 列数 从0开始
	 * @return
	 */
	public static double nansum(List<List<Double>> sumList, int col) {
		double sum = 0.0;
		for(int i = 0; i < sumList.size(); i++){// row
			List<Double> tempList = sumList.get(i);
			sum += tempList.get(col);
		}
		return sum;
	}
	
	/**
	 * 按列求平均值
	 * @param meanList
	 * @return
	 */
	public static List<Double> nanmean(List<List<Double>> meanList) {
		List<Double> sumList = nansum(meanList);
		for(int i = 0; i < sumList.size(); i++) {
			int rowNum = meanList.size();
			double colValue = sumList.get(i);
			double mean = colValue/rowNum;
			sumList.set(i, mean);
		}
		return sumList;
	}
	
	/**
	 * 两个矩阵平均值
	 * @param src 被加的矩阵
	 * @param dest 加入的目标矩阵
	 * @return dest
	 */
	public static List<List<Double>> listAvg(List<List<Double>> src, List<List<Double>> dest) {
		if(dest.size() == 0) {
			dest.addAll(src);
			return dest;
		}
		int rowNum = src.size() < dest.size()?src.size():dest.size();
		for(int iRow = 0; iRow < rowNum; iRow++) {
			List<Double> srcRow = src.get(iRow);
			List<Double> destRow = dest.get(iRow);
			int colNum = srcRow.size()<dest.size()?srcRow.size() : destRow.size();
			for(int iCol = 0; iCol < colNum; iCol++) {
				double srcVal = srcRow.get(iCol)==null?0.0d:srcRow.get(iCol);
				double destVal = destRow.get(iCol)==null?0.0d:destRow.get(iCol);
				double avg = (srcVal + destVal)/2;
				destRow.set(iCol, avg);
			}
		}
		return dest;
	}
	
	/**
	 * 两个矩阵相加
	 * @param src 被加的矩阵
	 * @param dest 加入的目标矩阵
	 * @return 
	 */
	public static List<List<Double>> listAdd(List<List<Double>> src, List<List<Double>> dest) {
		if(dest.size() == 0) {
			dest.addAll(src);
			return dest;
		}
		int rowNum = src.size() < dest.size()?src.size():dest.size();
		for(int iRow = 0; iRow < rowNum; iRow++) {
			List<Double> srcRow = src.get(iRow);
			List<Double> destRow = dest.get(iRow);
			int colNum = srcRow.size()<destRow.size()?srcRow.size() : destRow.size();
			for(int iCol = 0; iCol < colNum; iCol++) {
				double srcVal = srcRow.get(iCol)==null?0.0d:srcRow.get(iCol);
				double destVal = destRow.get(iCol)==null?0.0d:destRow.get(iCol);
				double sum = srcVal + destVal;
				destRow.set(iCol, sum);
			}
		}
		return dest;
	}
	
	/**
	 * 矩阵中的每个元素被除
	 * @param list
	 * @param divideNum
	 * @return
	 */
	public static List<List<Double>> listDivid(List<List<Double>> list, int divideNum) {
		for(int iRow = 0; iRow < list.size(); iRow++) {
			List<Double> row = list.get(iRow);
			for(int iCol = 0; iCol < row.size(); iCol++) {
				if(row.get(iCol) != null) {
					double value = row.get(iCol);
					value = value/divideNum;
					row.set(iCol, value);
				}else {
					row.set(iCol, 0.0d);
				}
				
			}
		}
		return list;
	}
	
	/**
	 * 两个矩阵相连
	 * @param src 
	 * @param dest
	 * @return 
	 */
	public static List<List<String>> listUnion(List<List<String>> src, List<List<Double>> dest) {
		if(src.size() == 0) {
			int rowNum = dest.size();
			for(int iRow = 0; iRow < rowNum; iRow++) {
				List<Double> row = dest.get(iRow);
				List<String> rowStr = new ArrayList<String>();
				for(int iCol = 0; iCol < row.size(); iCol++) {
					if(row.get(iCol) != null) {
						rowStr.add(row.get(iCol).toString());
					}else {
						rowStr.add(null);
					}
				}
				rowStr.add(null);
				src.add(rowStr);
			}
		}else {
			int rowNum = src.size() < dest.size()?src.size():dest.size();
			for(int iRow = 0; iRow < rowNum; iRow++) {
				List<String> srcRow = src.get(iRow);
				List<Double> destRow = dest.get(iRow);
				int colNum = destRow.size();
				for(int iCol = 0; iCol < colNum; iCol++) {
					srcRow.add(destRow.get(iCol).toString());
				}
				srcRow.add(null);
			}
		}
		return src;
	}
	

	public static void main(String[] args) {
		System.out.println(MathUtil.doubleFormat(1445.123, 2));
		System.out.println(MathUtil.doubleFormat(23.555, 2));
		System.out.println(MathUtil.doubleFormat(23.556, 2));
	}

	
}
