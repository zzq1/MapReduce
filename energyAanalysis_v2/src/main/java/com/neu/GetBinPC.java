package com.neu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.neu.utils.ConvertUtil;
import com.neu.utils.DateUtil;
import com.neu.utils.MathUtil;
import com.neu.vo.FanDataVO;
import com.neu.vo.TFByWsPowerVO;
/**
 * 获取测量功率曲线的Bin值
 * @author kangcc
 * @param normalData 前面处理完的正常数据
 * @return JavaPairRDD Map<风速+年月，FanDataVO（Bin区间点 | 平均风速 | 平均功率）>
 */
public class GetBinPC implements Serializable{

	private static final long serialVersionUID = 4678663267630841299L;
	
	public Map<String, List<List<Double>>> run(JavaRDD<FanDataVO> normalData) {
		
		JavaPairRDD <String, FanDataVO> mapData = normalData.mapToPair(new PairFunction<FanDataVO, String, FanDataVO>() {
			
			private static final long serialVersionUID = 8247230487025727732L;

			@Override
			public Tuple2<String, FanDataVO> call(FanDataVO t) throws Exception {
				//获取风场编号
				String farmNO = t.getFarmNo();
				String fanNO = t.getFanNo();
				//获取年月 
				String yearAndMonth = DateUtil.getMonthByDays(t.getTime());
				double ws_AVG = Double.parseDouble(t.getWt_WsAvg());
				//风速取整
				double evidenceWS = Math.floor(ws_AVG);
				//根据风速取整值确定区间
				if (ws_AVG < evidenceWS+0.25) {
					ws_AVG = evidenceWS;
				}
				if (ws_AVG >= evidenceWS+0.25 && ws_AVG < evidenceWS+0.75) {
					ws_AVG = evidenceWS+0.5;
				}
				if (ws_AVG >= evidenceWS+0.75) {
					ws_AVG = evidenceWS+1;
				}
				//为每个 key 加上 “_”方便后续拆解
				String key = farmNO + "_" +fanNO+"_"+yearAndMonth + "_" + ws_AVG;
				t.setCount(1.0);
				return new Tuple2<String, FanDataVO>(key, t);
			}
		});
		
		// 对数据进行汇总求出各区间内的数据条数、平均风速、平均功率
		JavaPairRDD<String, TFByWsPowerVO> NPCBinRDD = mapData.combineByKey(new Function<FanDataVO, TFByWsPowerVO>() {

			private static final long serialVersionUID = -3792092354794375166L;

			@Override
			public TFByWsPowerVO call(FanDataVO v1) throws Exception {
				
				TFByWsPowerVO TFByWsPVO = new TFByWsPowerVO();
				TFByWsPVO.setWt_WsAvg(v1.getWt_WsAvg());
				TFByWsPVO.setWt_PowerAvg(v1.getWt_PowerAvg());
				TFByWsPVO.setCount(1.0);
				return TFByWsPVO;
			}
		}, new Function2<TFByWsPowerVO, FanDataVO, TFByWsPowerVO>() {

			private static final long serialVersionUID = 1L;

			@Override
			public TFByWsPowerVO call(TFByWsPowerVO v1, FanDataVO v2)
					throws Exception {
				//计算平均风速
				double ws_Sum = Double.parseDouble(v1.getWt_WsAvg()) * v1.getCount() + Double.parseDouble(v2.getWt_WsAvg());
				double ws_Avg = ws_Sum/(v1.getCount() + v2.getCount());
				v1.setWt_WsAvg(ws_Avg+"");
				//计算平均功率
				double power_Sum = Double.parseDouble(v1.getWt_PowerAvg())*v1.getCount() + Double.parseDouble(v2.getWt_PowerAvg());
				double power_Avg = power_Sum/(v1.getCount() + v2.getCount());
				v1.setWt_PowerAvg(power_Avg+"");
				//Count 累加
				v1.setCount(v1.getCount() + v2.getCount());
				
				return v1;
			}
		}, new Function2<TFByWsPowerVO, TFByWsPowerVO, TFByWsPowerVO>() {
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public TFByWsPowerVO call(TFByWsPowerVO v1,
					TFByWsPowerVO v2) throws Exception {
				//计算平均风速
				double ws_Sum = Double.parseDouble(v1.getWt_WsAvg()) * v1.getCount() + Double.parseDouble(v2.getWt_WsAvg())*v2.getCount();
				double ws_Avg = ws_Sum/(v1.getCount() + v2.getCount());
				v1.setWt_WsAvg(ws_Avg+"");
				//计算平均功率
				double power_Sum = Double.parseDouble(v1.getWt_PowerAvg())*v1.getCount() + Double.parseDouble(v2.getWt_PowerAvg())*v2.getCount();
				double power_Avg = power_Sum/(v1.getCount() + v2.getCount());
				v1.setWt_PowerAvg(power_Avg+"");
				//Count 累加
				v1.setCount(v1.getCount() + v2.getCount());
				return v1;
			}
		});
		mapData = mapData.unpersist();
		// key=风场编号_风机编号_年月_分区
		Map<String, TFByWsPowerVO> result = NPCBinRDD.collectAsMap();
		NPCBinRDD = NPCBinRDD.unpersist();
		// 封装 结果 Map<年月,List<List<数据条数、风速、功率>>>
		Map<String, List<List<Double>>> resultMap = new HashMap<String, List<List<Double>>>();
		Set<String> binKeySet = result.keySet();
		for (String yearMonthBin : binKeySet) {
			TFByWsPowerVO vo = result.get(yearMonthBin);
//			String yearMonth = yearMonthBin.substring(0, yearMonthBin.indexOf("_"));
			String [] keyArr = yearMonthBin.split("_");
			String key  = keyArr[0] +"_"+ keyArr[1]+"_"+ keyArr[2];
			// 每个月的功率曲线
			List<List<Double>> monthBin = resultMap.get(key);
			if(monthBin == null) {
				monthBin = new ArrayList<List<Double>>();
			}
			List<Double> row = new ArrayList<Double>();
			row.add(vo.getCount());
			row.add(Double.parseDouble(vo.getWt_WsAvg()));
			double avgPower = MathUtil.doubleFormat(Double.parseDouble(vo.getWt_PowerAvg()), 2);
			row.add(avgPower);
			monthBin.add(row);
			resultMap.put(key, monthBin);
		}
		
		Set<String> yearMonthKeys = resultMap.keySet();
		for (String key : yearMonthKeys) {
			List<List<Double>> monthBinPC = resultMap.get(key);
			if(monthBinPC == null || monthBinPC.size() == 0) {
				continue;
			}
			monthBinPC = getListVOSort(monthBinPC, 1);
			monthBinPC = fillOutageSection(monthBinPC);
			resultMap.put(key, monthBinPC);
		}
		return resultMap;
	}
	
	/**
	 * 对 List<String> 去重
	 * @param list
	 * @return 去除重复后的 List
	 */
	public static List<String> RemoveDuplicates(List<String> list){
		
		List<String> tempList= new ArrayList<String>();  
	    for(String i:list){  
	        if(!tempList.contains(i)){  
	            tempList.add(i);  
	        }  
	    }  
		return tempList;
	}
	
	/**
	 * 找到功率曲线的中断区间并补全中断区间
	 * @param pcBin 功率曲线
	 * @return 补全后的功率曲线
	 */
	public static List<List<Double>> fillOutageSection(List<List<Double>> pcBin){
		List<List<Double>> newpcBin = new ArrayList<List<Double>>();
		for(int i = 0; i < pcBin.size() - 1; i++) {
			newpcBin.add(pcBin.get(i));
			double curWs = pcBin.get(i).get(1);// 当前行的风速
			double nextWs = pcBin.get(i+1).get(1);// 下一行的风速
			// 分别找出当前行和下一行风速属于哪个标准区间（步长0.5）
			double standardWs = Math.floor(curWs);
			double nextStandardWs = Math.floor(nextWs);
			if(curWs >= standardWs+0.25 && curWs < standardWs+0.75) {
				standardWs = standardWs + 0.5;
			}else if(curWs >=standardWs + 0.75) {
				standardWs = standardWs + 1;
			}
			
			if(nextWs >= nextStandardWs+0.25 && nextWs < nextStandardWs + 0.75) {
				nextStandardWs = nextStandardWs + 0.5;
			}else if(nextWs >= nextStandardWs + 0.75){
				nextStandardWs = nextStandardWs + 1;
			}
			// 中断区间数
			double sectionInterval = (nextStandardWs - standardWs)/0.5 - 1;
			// 补全中断区间
			for(int j = 0; j < sectionInterval; j++) {
				List<Double> newRow = new ArrayList<Double>();
				newRow.add(0.0d);
				newRow.add(Double.NaN);
				newRow.add(Double.NaN);
				newpcBin.add(newRow);
			}
		}
		newpcBin.add(pcBin.get(pcBin.size() - 1));
		return newpcBin;
	}
	
	/**
	 * 对 List<List<Double>> 按照 index 列进行升序排序
	 * 
	 * @param sortList
	 *            要进行排序的 List 集合
	 * @param index
	 *            排序根据的列
	 * @return
	 */
	public List<List<Double>> getListVOSort(List<List<Double>> sortList,int index) {
		for (int i = 0; i < sortList.size(); i++) {// 趟数
			if(sortList.get(i) == null) continue;
			for (int j = 0; j < sortList.size() - i - 1; j++) {// 比较次数
				List<Double> temp = new ArrayList<Double>();
				double curVal = 0.0d;
				if(sortList.get(j)!= null && sortList.get(j).size() > 0) {
					curVal = sortList.get(j).get(index);
				}
				double nextVal = 0.0d;
				if(sortList.get(j + 1) != null && sortList.get(j+1).size() > 0) {
					nextVal = sortList.get(j+1).get(index);
				}
				if (curVal > nextVal) {
					temp = sortList.get(j);
					sortList.set(j, sortList.get(j + 1));
					sortList.set(j + 1, temp);
				}
			}
		}
		return sortList;
	}
	
	public static void main(String[] args) {
//		List<List<String>> pcBin = ConvertUtil.readCSVFile("C:\\Users\\DLM\\Desktop\\效能分析系统资料\\DataAnalysisMainV4_0\\数据\\NPCBin.csv");
//		List<List<Double>> pcBinDou = ConvertUtil.convertStrToDou(pcBin);
//		pcBinDou = fillOutageSection(pcBinDou);
//		String[] header = {"count","ws","pow"};
//		ConvertUtil.writeCsvFile(header, pcBinDou, "C:\\Users\\DLM\\Desktop\\效能分析系统资料\\DataAnalysisMainV4_0\\数据\\fillOutageSection.csv");
		
//		List<List<String>> normalData = ConvertUtil.readCSVFile("C:\\Users\\DLM\\Desktop\\效能分析系统资料\\DataAnalysisMainV4_0\\数据\\NormalData.csv");
//		List<List<Double>> normalDataDou = ConvertUtil.convertStrToDou(normalData);
		SparkConf conf = new SparkConf();
		conf.setAppName("dataAnalysis_v2");
		conf.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<FanDataVO> normalData = sc.textFile("/dataAnalysis/testData/duliming/NormalData.csv").map(
				new Function<String, FanDataVO>() {

					private static final long serialVersionUID = 9030924689960030210L;

					@Override
					public FanDataVO call(String row) throws Exception {
						String[] rowArr = row.split(",");
						FanDataVO vo = new FanDataVO();
						vo.setTime(rowArr[0]);
						vo.setWt_WsAvg(rowArr[1]);
						vo.setWt_PowerAvg(rowArr[2]);
						return vo;
					}
				});
		GetBinPC getBinPC = new GetBinPC();
		Map<String, List<List<Double>>> result = getBinPC.run(normalData);
		String[] header = {"num","ws","pow"};
		ConvertUtil.writeCsvFile(header, result.get("20147"), "C:\\Users\\DLM\\Desktop\\效能分析系统资料\\DataAnalysisMainV4_0\\数据\\NPCBin.csv");
	}
}
