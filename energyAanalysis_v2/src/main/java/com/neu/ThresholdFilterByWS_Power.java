package com.neu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import com.neu.common.AnalysisException;
import com.neu.utils.CommonUtils;
import com.neu.vo.FanDataVO;
import com.neu.vo.FarmDicVO;

/**
 * 风速-功率阈值筛选器
 * 
 * @author kangcc
 *
 */

public class ThresholdFilterByWS_Power implements Serializable{

	private static final long serialVersionUID = 6642699511649163170L;

	private static Logger log = Logger.getLogger(ThresholdFilterByWS_Power.class);

	public static final String KEY_NORMALDATA = "NormalData";
	public static final String KEY_ABNORMALDATA = "AbnormalData";
	public static final String FARMPC = "FarmPC";
	public static final String REFERBIN = "ReferBin";
	private Properties prop = Main.prop;
	GetReferBin getReferBin = new GetReferBin();
	GetBinPC getBinPC = new GetBinPC();

	/**
	 * 风速-功率阈值筛选器
	 * 
	 * @param data
	 *            全年数据
	 * @param farmPC
	 *            初始化参数，全年平均功率曲线
	 * @param wtParam
	 *            初始化参数
	 * @param signPrefixes 风场台账数据(key=风机编号，value=台账数据)
	 * @return NormalData 正常数据 AbnormalData 异常数据
	 * @throws AnalysisException 
	 */
	public Map<String, Object> run(JavaRDD<FanDataVO> data,
			Map<String, List<List<Double>>> GPC, 
			Broadcast<Map<String, FarmDicVO>> signPrefixes,
			int iteraTimes) throws AnalysisException {
		Map<String, FarmDicVO> farmDicInfos = signPrefixes.value();// 台账数据
		log.info("2.3 风速-功率阈值筛选算法开始。");
		JavaRDD<FanDataVO> NormalData = null;
		JavaRDD<FanDataVO> AbnormalData = null;
		Map<String, List<List<Double>>> ReferBin = null;
		Map<String, List<List<Double>>> farmPC = new HashMap<String, List<List<Double>>>();
		
		Map<String, List<List<Double>>> UpPCBinMap = new HashMap<String, List<List<Double>>>();
		Map<String, List<List<Double>>> DownPCBinMap = new HashMap<String, List<List<Double>>>();
		Set<String> keySet = GPC.keySet();
		for (String key : keySet) {
			List<List<Double>> UpPCBin = new ArrayList<List<Double>>(); // 上限
			List<List<Double>> DownPCBin = new ArrayList<List<Double>>(); // 下限功率曲线
			List<List<Double>> farmPCList = GPC.get(key);
			for (int i = 0; i < farmPCList.size(); i++) {

				List<Double> tempList = new ArrayList<Double>();
				for (int j = 0; j < farmPCList.get(i).size(); j++) {
					double tempNum = farmPCList.get(i).get(j);
					tempList.add(tempNum);
				}
				DownPCBin.add(tempList);
			}
			for (int i = 0; i < farmPCList.size(); i++) {

				List<Double> tempList = new ArrayList<Double>();
				for (int j = 0; j < farmPCList.get(i).size(); j++) {
					double tempNum = farmPCList.get(i).get(j);
					tempList.add(tempNum);
				}
				UpPCBin.add(tempList);
			}

			// ------------- 定义上限PC ---------
			for (int i = 0; i < UpPCBin.size(); i++) {
				double value = UpPCBin.get(i).get(2 - 1) * 1.4;
				if (value <= 0) {
					UpPCBin.get(i).set(2 - 1, 10.0);
				} else {
					UpPCBin.get(i).set(2 - 1, value);
				}
			}

			// ------------- 定义下限PC ---------
			for (int i = 0; i < DownPCBin.size(); i++) {

				double value = DownPCBin.get(i).get(1) * 0.8;
				DownPCBin.get(i).set(1, value);
				// % 前面赋为0
				if (DownPCBin.get(i).get(0) <= 2) {
					DownPCBin.get(i).set(2 - 1, 0.0);
				}
			}
			double max = getMax(DownPCBin, 1);
			int indexOfMax = 0;
			for (int i = 0; i < DownPCBin.size(); i++) {
				if (DownPCBin.get(i).get(2 - 1) == max) {
					indexOfMax = i;
					break;
				}
			}
			// %----- 优化上限PC和下限PC后面的bin区间 -----
			// 只需要获取到第一次出现最大值的索引即可
			DownPCBin.get(indexOfMax).set(2 - 1,
					DownPCBin.get(indexOfMax).get(2 - 1) * 1.1);
			DownPCBin.get(indexOfMax + 1).set(2 - 1,
					DownPCBin.get(indexOfMax).get(2 - 1) * 1.1);
			DownPCBin.get(indexOfMax + 2).set(2 - 1,
					DownPCBin.get(indexOfMax).get(2 - 1) * 1.2);
			DownPCBin.get(indexOfMax + 3).set(2 - 1,
					DownPCBin.get(indexOfMax).get(2 - 1) * 1.3);
			DownPCBin.get(indexOfMax + 4).set(2 - 1,
					DownPCBin.get(indexOfMax).get(2 - 1) * 1.4);
			max = getMax(DownPCBin, 1);// 获取最大值
			for (int i = indexOfMax + 5; i < DownPCBin.size(); i++) {
				DownPCBin.get(i).set(2 - 1, max);
			}
			for (int i = indexOfMax; i < UpPCBin.size(); i++) {
				UpPCBin.get(i).set(2 - 1,
						UpPCBin.get(indexOfMax - 1).get(2 - 1));
			}
			// key=风场ID_风场机型_年月
			UpPCBinMap.put(key, UpPCBin);
			DownPCBinMap.put(key, DownPCBin);
		}
		
		Map<String, List<List<Double>>> beforReferBinMap = null;
		
		for (int iTime = 1; iTime < 20; iTime++) {
			
			NormalData = data.filter(new ThresholdFilterByWS_PowerFunc.func(
					UpPCBinMap, DownPCBinMap, iTime, ReferBin,signPrefixes,KEY_NORMALDATA, false));
			AbnormalData = data.filter(new ThresholdFilterByWS_PowerFunc.func(
					UpPCBinMap, DownPCBinMap, iTime, ReferBin,signPrefixes,KEY_ABNORMALDATA, false));
			
			// 到达定义的迭代次数，停止迭代
			if (iTime == iteraTimes + 1) {
				break;
			}

			log.info("调用 GetBinPC 和 GetReferBin 方法对功率曲线进行拟合");
			Map<String, List<List<Double>>> NPCBin = getBinPC.run(NormalData);
					
			// 第一次迭代时 farmPC 为空需要用GPC进行拟合
			if (farmPC.size() == 0) {
				ReferBin = getReferBin.run(NPCBin, GPC, signPrefixes, false);
			}else {
				ReferBin = getReferBin.run(NPCBin, farmPC, signPrefixes, true);
			}
			
			String avgWithBefore = prop.getProperty("Referbin.iterator.avgWithBefore");
			if(avgWithBefore.equals("1") && iTime > 2){
				ReferBin = CommonUtils.referBinAvg(beforReferBinMap, ReferBin);
			}
			beforReferBinMap = ReferBin;
			
			farmPC.clear(); // 清空 farmPC 防止出现多余 key 的情况
			// 计算 farmPC 值
			farmPC = ReferBin;
			NormalData = NormalData.unpersist();
			AbnormalData = AbnormalData.unpersist();
			log.info("完成第" + iTime + "次迭代");
		}
		log.info("完成根据风速 - 功率阈值筛选");
		// 封装结果 Map
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put(KEY_NORMALDATA, NormalData);
		resultMap.put(KEY_ABNORMALDATA, AbnormalData);
		resultMap.put(FARMPC, farmPC);
		return resultMap;
	}
	/**
	 * 获取 List<Double> 内最大值
	 * 
	 * @param list
	 * @param column
	 *            列数 0开始
	 * @return List<Double> 内的最大值
	 */
	public static double getMax(List<List<Double>> list, int column) {
		double max = 0.0;
		for (int j = 0; j < list.size(); j++) {
			if (list.get(j).get(column) > max) {
				max = list.get(j).get(column);
			}
		}
		return max;
	}
	
}
