package com.neu.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import com.neu.common.AnalysisException;

public class ConvertUtil {

	private static Logger log = Logger.getLogger(ConvertUtil.class);
	
	/**
	 * List<List<Double>>转换List<List<String>>
	 * @param values
	 * @return
	 */
	public static List<List<String>> convertDouToStr(List<List<Double>> values){
		List<List<String>> resultList = new ArrayList<List<String>>();
		List<String> douList = null;
		for(List<Double> value : values){
			douList = new ArrayList<String>();
			for(Double douValue : value){
				if(!douValue.isNaN()){
					douList.add(douValue.toString());
				} else {
					douList.add("0.0");
				}
			}
			resultList.add(douList);
		}
		douList = null;
		
		return resultList;
	}
	
	/**
	 * List<List<String>>转换List<List<Double>>
	 * @param values
	 * @return
	 */
	public static List<List<Double>> convertStrToDou(List<List<String>> values) {
		List<List<Double>> resultList = new ArrayList<List<Double>>();
		List<Double> douList = null;
		for(List<String> value : values){
			douList = new ArrayList<Double>();
			for(String douValue : value){
				if(douValue == null) {
					douList.add(0.0d);
				}else {
					douList.add(Double.parseDouble(douValue));
				}
			}
			resultList.add(douList);
		}
		douList = null;
		return resultList;
	}
	
	/**
	 * List<List<String>>转换List<List<Double>>
	 * @param values
	 * @param endIndex 子集合的结束索引
	 * @return
	 */
	public static List<List<Double>> convertStrToDou(List<List<String>> values, int endIndex) {
		List<List<Double>> resultList = new ArrayList<List<Double>>();
		List<Double> douList = null;
		for(List<String> value : values){
			douList = new ArrayList<Double>();
			value = value.subList(0, endIndex);
			for(String douValue : value){
				if(douValue == null) {
					douList.add(0.0d);
				}else {
					douList.add(Double.parseDouble(douValue));
				}
			}
			resultList.add(douList);
		}
		douList = null;
		return resultList;
	}
	
	/**
	 * List<String>转换List<Double>
	 * @param values
	 * @return
	 */
	public static List<Double> convertStrToDouList(List<String> values){
		List<Double> resultList = new ArrayList<Double>();
		for(String value : values) {
			if(StringUtils.isNotBlank(value)) {
				resultList.add(Double.parseDouble(value));
			}else {
				resultList.add(0.0d);
			}
		}
		return resultList;
	}
	
	/**
	 * List<Double>转换List<String>
	 * @param values
	 * @return
	 */
	public static List<String> convertDoubleToStrList(List<Double> values){
		List<String> resultList = new ArrayList<String>();
		if(values == null) {
			return resultList;
		}
		for(Double value : values) {
			if(value != null) {
				resultList.add(value.toString());
			}else {
				resultList.add("0.0");
			}
		}
		return resultList;
	}
	
	/**
	 * 把DataFrame转换成List<List<Double>>集合
	 * @param df
	 * @return
	 */
	public static List<List<Double>> convertDFToListDouble(DataFrame df){
		List<List<Double>> resultList =  new ArrayList<List<Double>>();
		List<Row> tempRow = df.collectAsList();
		if(tempRow.size() > 0){
			List<Double> tempList = null;
			for(int i=0;i<tempRow.size();i++){
				tempList = new ArrayList<Double>();
				for(int j=0;j < df.columns().length;j++){
					try {
						Object value = tempRow.get(i).get(j);
						if(value == null) {
							tempList.add(0.0d);
							continue;
						}
						if(value instanceof String) {
							String tmp = (String)value;
							tmp = tmp.length()==0?"0.0":tmp;
							tempList.add(Double.parseDouble(tmp));
						}
						if(value instanceof Double) {
							tempList.add((Double)value);
						}
					} catch (NumberFormatException e) {
						tempList.add(0.0);
					} catch(ClassCastException e) {
						tempList.add(0.0);
					}
				}
				resultList.add(tempList);
			}
			tempList = null;
		}
		return resultList;
	}
	
	/**
	 * 把DataFrame转换成List<List<String>>集合
	 * @param df
	 * @return
	 */
	public static List<List<String>> convertDFToListString(DataFrame df){
		List<List<String>> resultList =  new ArrayList<List<String>>();
		
		if(df.count()>0){
			Row tempRow[] = df.head((int) df.count());
			List<String> tempList = null;
			for(int i=0;i<tempRow.length;i++){
				
				tempList = new ArrayList<String>();
				for(int j=0;j < df.columns().length;j++){
					tempList.add(tempRow[i].getString(j));
				}
				resultList.add(tempList);
			}
			tempList = null;
		}
		return resultList;
	}
	
	/**
	 * 把List<List<Double>> 类型 写出到本地的CSV中
	 * @param FILE_HEADER
	 * @param FrontBin
	 * @param fileName
	 */
	//CSV文件分隔符
	private static String NEW_LINE_SEPARATOR = "\n";
	public static void writeCsvFile(Object [] FILE_HEADER, List<List<Double>> FrontBin,String fileName){

		FileWriter fileWriter = null;
		CSVPrinter csvFilePrinter = null;
		//创建 CSVFormat
		CSVFormat csvFileFormat = CSVFormat.newFormat(',').withRecordSeparator(NEW_LINE_SEPARATOR);
		try {
			//初始化FileWriter
			fileWriter = new FileWriter(fileName, true);
			//初始化 CSVPrinter
	        csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
	        //创建CSV文件头
	        if(FILE_HEADER != null) {
	        	csvFilePrinter.printRecord(FILE_HEADER);
	        }
			// 遍历List写入CSV
			for (List<Double> list : FrontBin) {
	            csvFilePrinter.printRecord(list);
			}
			log.info(fileName+"文件创建成功~~~");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
                e.printStackTrace();
			}
		}
	}
	
	/**
	 * 将List<List<String>>数据写入到HDFS中。
	 * 
	 * @param FILE_HEADER 表头
	 * @param FrontBin 写入数据
	 * @param fileName 文件路径
	 * @throws IOException
	 * @throws AnalysisException 
	 */
	public static void writeCsvToHDFSFile(Object [] FILE_HEADER, List<List<String>> FrontBin,String fileName) throws AnalysisException{
		OutputStream out = null;
		OutputStreamWriter outWriter = null;
		CSVPrinter csvFilePrinter = null;
		//创建 CSVFormat
		CSVFormat csvFileFormat = CSVFormat.newFormat(',').withRecordSeparator(NEW_LINE_SEPARATOR);
		try {
			HDFSUtil hdfs = new HDFSUtil();
			//初始化FileWriter
			out = hdfs.getHDFSOutputStream(fileName);
			outWriter = new OutputStreamWriter(out);
			//初始化 CSVPrinter
	        csvFilePrinter = new CSVPrinter(outWriter, csvFileFormat);
	        //创建CSV文件头
	        if(FILE_HEADER != null) {
	        	csvFilePrinter.printRecord(FILE_HEADER);
	        }
			// 遍历List写入CSV
			for (List<String> list : FrontBin) {
	            csvFilePrinter.printRecord(list);
			}
			log.info(fileName+"文件创建成功~~~");
		} catch (Exception e) {
			log.error(fileName+"创建失败。",e);
			throw new AnalysisException(fileName+"创建失败。",e);
		} finally {
			try {
				outWriter.flush();
				outWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
                e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * 把List<List<Double>> 类型 写出到本地的CSV中
	 * @param FILE_HEADER
	 * @param FrontBin
	 * @param fileName
	 */
	//CSV文件分隔符
	private static String LINE_SEPARATOR = "\n";
	public static void writeStringCsvFile(Object [] FILE_HEADER, List<List<String>> FrontBin,String fileName){

		FileWriter fileWriter = null;
		CSVPrinter csvFilePrinter = null;
		//创建 CSVFormat
        //CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(LINE_SEPARATOR);
        CSVFormat csvFileFormat = CSVFormat.newFormat(',').withRecordSeparator(LINE_SEPARATOR);
		try {
			//初始化FileWriter
			fileWriter = new FileWriter(fileName,true);
			//初始化 CSVPrinter
	        csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
	        //创建CSV文件头
	        if(FILE_HEADER != null) {
	        	csvFilePrinter.printRecord(FILE_HEADER);
	        }
			// 遍历List写入CSV
			for (List<String> list : FrontBin) {
	            csvFilePrinter.printRecord(list);
			}
			log.info(fileName+"文件创建成功~~~");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
                e.printStackTrace();
			}
		}
	}
	
	/**
	 * 读取CSV文件
	 * @param fileName csv文件路径
	 * @return
	 */
	public static List<List<String>> readCSVFile(String fileName){

		List<List<String>> csvData = new ArrayList<List<String>>();
		Reader in;
		try {
			in = new FileReader(fileName);
			Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
			for (CSVRecord record : records) {
				List<String> row = new ArrayList<String>();
				Iterator<String> it = record.iterator();
				while(it.hasNext()) {
					String data = it.next();
					row.add(data);
				}
				csvData.add(row);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return csvData;
	}
	
	public static final String DF_SAVE_PATH = "/dataAnalysis/tmp/";
	
	/**
	 * 保存DataFrame到指定的HDFS路径下
	 * @param df DataFrame对象。
	 * @param sqlContext SQLContext对象。
	 * 保存后重新加载的新的DataFrame对象
	 */
	public static DataFrame transformDataFrame(DataFrame df, SQLContext sqlContext){
		String path = DF_SAVE_PATH + System.currentTimeMillis() + "/";
		saveDataFrame(df, path);
		DataFrame loadDF = loadDataFrame(path + "part-00000", sqlContext);
		return loadDF;
	}
	
	/**
	 * 保存DataFrame到指定的HDFS路径下
	 * @param df DataFrame对象
	 * @param path HDFS目录
	 */
	@SuppressWarnings("deprecation")
	public static void saveDataFrame(DataFrame df, String path) {
		HashMap<String, String> options = new HashMap<String, String>();
		options.put("header", "true");
		options.put("delimiter", ",");
		options.put("path", path);
		df.save("com.databricks.spark.csv", SaveMode.Overwrite, options);
	}
	
	/**
	 * 加载指定路径下的DataFrame(只加载通过DataFrame.save保存的文件)
	 * @param path 指定目录
	 * @param sqlContext SQLContext对象
	 * @return 指定路径下加载出来的DataFrame
	 */
	@SuppressWarnings("deprecation")
	public static DataFrame loadDataFrame(String path, SQLContext sqlContext) {
		HashMap<String, String> options = new HashMap<String, String>();
		options.put("header", "true");
		options.put("delimiter", ",");
		options.put("path", path);
		DataFrame df = sqlContext.load("com.databricks.spark.csv",options);
		return df;
	}
	
	public static void main(String[] args) {
//		JavaSparkContext sc = new JavaSparkContext("local", "dataAnalysis");
//		SQLContext sqlContext = new SQLContext(sc);
//		HashMap<String, String> options = new HashMap<String, String>();
//		options.put("header", "true");
//		options.put("delimiter", ",");
//		options.put("path", "/dataAnalysis/inputData/67.csv");
//		long startTime = System.currentTimeMillis();
//		DataFrame df = sqlContext.load("com.databricks.spark.csv",options);
//		DataFrame tDF = transformDataFrame(df, sqlContext);
//		long count = tDF.count();
//		System.out.println(count);
//		long endTime = System.currentTimeMillis();
//		System.out.println("耗时：" + (endTime - startTime));
//		List<List<String>> csvData = readCSVFile("C:\\Users\\DLM\\Desktop\\效能分析系统资料\\DataAnalysisMainV4_0\\数据\\GetBinPC_spark.csv");
//		for(int i = 0; i < csvData.size(); i++) {
//			List<String> row = csvData.get(i);
//			System.out.println(Arrays.toString(row.toArray()));
//		}
	}
}
