package com.neu;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple3;

import com.neu.utils.DateUtil;
import com.neu.vo.FanDataVO;
import com.neu.vo.FarmDicVO;

/**
 * 加载数据
 *
 */
public class DataLoad implements Serializable {

	private static final long serialVersionUID = 6532580670087454672L;

	/**
	 * 从Hive表中加载数据
	 * @param signPrefixes 
	 * 
	 * @param farmNos
	 *            风场编号
	 * @param DataLineNo
	 *            数据列索引
	 * @param signPrefixes
	 *            风场台账数据
	 * 
	 * @return 加载的数据集
	 */
	public JavaRDD<FanDataVO> loadHiveData(SparkContext sc, String farmId, 
			String startTime, String endTime, final Broadcast<Map<String, FarmDicVO>> signPrefixes) {
		HiveContext hc = new HiveContext(sc);
		String sql = "select fan_no,data_date,wind_speed,power,rotor_rs,wt_t from dataanalysis.tenmindata" + " where fc in("
				+ farmId + ") and data_date>='" + startTime
				+ "' and data_date<'" + endTime + "'";
		DataFrame df = hc.sql(sql);
		JavaRDD<Row> hiveData = df.toJavaRDD();
		JavaRDD<FanDataVO> data = hiveData.map(new Function<Row, FanDataVO>() {

			private static final long serialVersionUID = 1117007054306099732L;

			@Override
			public FanDataVO call(Row row) throws Exception {
				FanDataVO vo = new FanDataVO();
				vo.setFanNo(row.getString(0));// 风机编号
				FarmDicVO farmDicVO = signPrefixes.value().get(
						vo.getFanNo());
				vo.setFanType(farmDicVO.getFanType());// 风机类型
				vo.setFarmNo(farmDicVO.getFarmNo());// 风场ID
				
				String timeString = row.getString(1);
				String time = DateUtil.getTimes(timeString);
				vo.setTime(time);// 时间
				vo.setYear(Integer.parseInt(timeString.substring(0, 4)));// 年
				vo.setMonth(Integer.parseInt(timeString.substring(5,7)));// 月
				vo.setDay(Integer.parseInt(timeString.substring(8,10)));// 日
				
				double ws = row.getDouble(2);
				vo.setWt_WsAvg(String.valueOf(ws));// 风机风速
				
				double pow = row.getDouble(3);
				vo.setWt_PowerAvg(String.valueOf(pow));// 风机功率
				
				double rotateSpeed = row.getDouble(4);
				vo.setRs(String.valueOf(rotateSpeed));// 风轮转速
				
				double roomTem = row.getDouble(5);
				vo.setWt_T(String.valueOf(roomTem));
				
				return vo;
			}
		});
		JavaRDD<FanDataVO> coalesceData = data.coalesce(60).persist(StorageLevel.MEMORY_ONLY_SER());
		data = data.unpersist();
		return coalesceData;
	}
	
	/**
	 * 从HDFS上加载数据集
	 * 
	 * @param sc
	 *            JavaSparkContext对象
	 * @param path
	 *            文件在HDFS上的路径
	 * @param DataLineNo
	 *            数据列索引
	 * @param signPrefixes
	 *            风场台账数据
	 * 
	 * @return 加载的数据集
	 */
	public JavaRDD<FanDataVO> loadHDFSData(JavaSparkContext sc, String path,
			final Broadcast<Map<String, FarmDicVO>> signPrefixes) {
		JavaRDD<FanDataVO> Data = sc.textFile(path).map(
				new Function<String, FanDataVO>() {

					private static final long serialVersionUID = 9030924689960030210L;

					@Override
					public FanDataVO call(String row) throws Exception {
						String[] rowArr = row.split(",");
						if (rowArr[0].toLowerCase().trim()
								.equals("tenmindata.sjly"))
							return new FanDataVO();
						FanDataVO vo = new FanDataVO();
						vo.setFanNo(rowArr[1]);// 风机编号
						FarmDicVO farmDicVO = signPrefixes.value().get(
								vo.getFanNo());
						vo.setFanType(farmDicVO.getFanType());// 风机类型
						vo.setFarmNo(farmDicVO.getFarmNo());// 风场ID
						
						String time = DateUtil.getTimes(rowArr[2]);
						vo.setTime(time);// 时间
						Tuple3<Integer, Integer, Integer> yearMonthDay = DateUtil
								.getDateByDays(time);
						vo.setTime(time);
						vo.setYear(yearMonthDay._1());// 年
						vo.setMonth(yearMonthDay._2());// 月
						vo.setDay(yearMonthDay._3());// 日
						
						double ws = Double.parseDouble(rowArr[4]);
						vo.setWt_WsAvg(String.valueOf(ws));// 风机风速
						
						double pow = Double.parseDouble(rowArr[22]);
						vo.setWt_PowerAvg(String.valueOf(pow));// 风机功率
						
						double rotateSpeed = Double.parseDouble(rowArr[6]);
						vo.setRs(String.valueOf(rotateSpeed));// 风轮转速
						
						double roomTem = Double.parseDouble(rowArr[12]);
						vo.setWt_T(String.valueOf(roomTem));
						
						return vo;
					}
				});
		return Data;
	}

	public static void main(String[] args) {
	}
}
