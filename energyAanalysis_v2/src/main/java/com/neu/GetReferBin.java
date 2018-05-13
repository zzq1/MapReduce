package com.neu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;

import com.neu.common.AnalysisException;
import com.neu.vo.FarmDicVO;
/**
 * 根据实测功率曲线和全场平均功率曲线进行曲线拟合
 * @author kangcc
 *
 */
public class GetReferBin implements Serializable{

	private static final long serialVersionUID = 1L;

	private static Logger log = Logger.getLogger(GetReferBin.class);

	CompleteCheck completeCheck = new CompleteCheck();

	public static final String Key_ReferBin = "ReferBin";
	public static final String Key_NPCBin = "NPCBin";
	public static final String Key_UP = "UP";
	public static final String Key_DOWN = "DOWN";
	/**
	 * 根据实测功率曲线和全场平均功率曲线进行曲线拟合
	 * @param NPCBin 计算后每月功率曲线
	 * @param PCBinFarm 全场功率曲线（key=风场ID_机型_年月）
	 * @param cutOutWS 切出风速
	 * @param wtParam 初始化文件指标参数
	 * @param signPrefixes 
	 * @return Map<年月，功率曲线List<List<Double>>>
	 * @throws AnalysisException 
	 */
	public  Map<String, List<List<Double>>> run(
			Map<String, List<List<Double>>> NPCBin, 
			Map<String, List<List<Double>>> PCBinFarm, 
			Broadcast<Map<String, FarmDicVO>> signPrefixes, 
			boolean flag) throws AnalysisException{

		Map<String,List<List<Double>>> result = new HashMap<String, List<List<Double>>>();
		log.info("执行  GetReferBin 方法");

		Set<String> keySet = NPCBin.keySet();
		Iterator<String> keyIt = keySet.iterator();
		while(keyIt.hasNext()){
			// key=风场ID_风机编号_年月
			String key = (String)keyIt.next();
			String [] keyArray = key.split("_");
			FarmDicVO farmDicVO = signPrefixes.value().get(keyArray[1]);
			
			String newKey = "";
			if (flag) {
				newKey = keyArray[0]+"_" + keyArray[1] +"_"+keyArray[2];//风场ID_风机_年月
			}else {
				newKey = keyArray[0]+"_" + farmDicVO.getFanType();//风场ID_风机类型
			}
			
			List<List<Double>> ReferBin = getResult(NPCBin.get(key), PCBinFarm.get(newKey), 20);
			Map<String, List<List<Double>>> upAndDown = getUpAndDown(ReferBin);
			if (upAndDown == null || upAndDown.size() == 0) {
				continue;
			}
			List<List<Double>> UpPCBin = upAndDown.get(Key_UP); //上限 List
			List<List<Double>> DownPCBin = upAndDown.get(Key_DOWN); //下限 List
			
			log.info("调用 getResult 方法计算 ReferBin ");
			//重新将上限值与下限值存入 ReferBin 的第三列和第四列
			for (int i = 0; i < UpPCBin.size(); i++) {
				ReferBin.get(i).add(UpPCBin.get(i).get(1));
			}
			for (int i = 0; i < DownPCBin.size(); i++) {
				ReferBin.get(i).add(DownPCBin.get(i).get(1));
			}
			result.put(key, ReferBin);
		}
		return result;
	}
	/**
	 * 根据实际功率曲线对每月的功率曲线进行拟合
	 * @param NPCBin 计算后的每月功率曲线
	 * @param PCBinFarm 实际功率曲线
	 * @param cutOutWS 切出风速
	 * @return
	 */
	public List<List<Double>> getResult(List<List<Double>> NPCBin, List<List<Double>> PCBinFarm,double cutOutWS) {
		
		List<List<Double>> ReferBin = new ArrayList<List<Double>>();
		NPCBin = completeCheck.run(NPCBin, 1, 2);
		List<List<Double>> FrontBin = new ArrayList<List<Double>>();

		// -----------插补前面区间--------------
		if(NPCBin == null || NPCBin.size() == 0) {
			return NPCBin;
		}
		double numFrontBin = Math.round((NPCBin.get(0).get(1)) / 0.5);
		for (int iBin = 1; iBin <= numFrontBin; iBin++) {

			List<Double> temp = new ArrayList<Double>();
			temp.add((double) 0);
			temp.add((iBin - 1) * 0.5);
			temp.add((double) 0);
			FrontBin.add(temp);
		}

		FrontBin.addAll(NPCBin);
		NPCBin = FrontBin;

		// ------------------------- 对不合理区间进行插补或剔除 ----------------------

		int numBinNPC = NPCBin.size();
		if (numBinNPC >= 4) {
			double lastLastPower = NPCBin.get(1).get(2);
			double lastPower = NPCBin.get(2).get(2);

			for (int iBin = 4; iBin <= numBinNPC; iBin++) {

				double diffPower1 = lastPower - lastLastPower;
				double diffPower2 = NPCBin.get(iBin - 1).get(2) - lastPower;
				double diffPower3 = NPCBin.get(iBin - 1).get(2) - lastLastPower;
				double diffWS = NPCBin.get(iBin - 1).get(1) - NPCBin.get(iBin - 2).get(1);
				// %----- 功率连续下降，则剔除自下降处bin区间到最后的bin区间 -----
				if (diffPower1 < 0 && diffPower2 < 0 || diffWS > 1
						|| NPCBin.get(iBin - 2).get(1) == 0) {
					for (int i = iBin - 1; i <= NPCBin.size(); i++) { // ???
						NPCBin.remove(i - 1);
						i--;
					}
					break;
				} else {
					// %----- 功率下降后又升高，则对下降的bin区间功率进行插补 -----
					if (diffPower1 < 0 && diffPower2 > 0) {
						// % 第2、3个点的功率均赋予第1个点的功率
						if (diffPower3 < 0) {
							NPCBin.get(iBin - 2).set(3 - 1,
									NPCBin.get(iBin - 3).get(3 - 1));

							NPCBin.get(iBin - 1).set(3 - 1,
									NPCBin.get(iBin - 3).get(3 - 1));
						}
						// % 第2个点功率小于第1个点功率，但第3个点大于第1个点功率
						else {

							NPCBin.get(iBin - 2).set(
									2 - 1,
									(NPCBin.get(iBin - 3).get(1) + NPCBin.get(
											iBin - 1).get(1)) / 2);

							NPCBin.get(iBin - 2).set(
									3 - 1,
									(NPCBin.get(iBin - 3).get(2) + NPCBin.get(
											iBin - 1).get(2)) / 2);

						}
					} else {
						// % 如果最后一个Bin区间比前一个Bin区间功率小，那么剔除最后一个Bin区间
						if (diffPower1 > 0 && diffPower2 < 0
								&& iBin == numBinNPC) {
							int length = numBinNPC;
							NPCBin.remove(length - 1);
							break;
						}
					}
				}

				lastLastPower = NPCBin.get(iBin - 2).get(2);
				lastPower = NPCBin.get(iBin - 1).get(2);
			}
		}
		// % 完整化bin区间
		numBinNPC = NPCBin.size();
		int numBinGurantee = PCBinFarm.size();

		// % 找到参考功率曲线功率大于处理后的NPCBin的最后Bin区间功率
		List<Integer> temp = new ArrayList<Integer>();

		for (int i = 1; i <= numBinGurantee; i++) {
			if (PCBinFarm.get(i - 1).get(1) > NPCBin.get(numBinNPC - 1).get(2)) {
				temp.add(i);
			}
		}
		List<List<Double>> RearBin = new ArrayList<List<Double>>();
		if (temp.size() > 0) {
			// % 找到这些功率值与NPCBin的最后Bin区间功率最接近的一个
			int k = Collections.min(temp);
			// % 待插补的尾部Bin区间的功率序列数组
			for (int j = (k + 1); j <= numBinGurantee; j++) {
				List<Double> list9 = new ArrayList<Double>();
				list9.add(0.0);
				list9.add(PCBinFarm.get(j - 1).get(1));
				RearBin.add(list9);
			}
			// 得出实际功率曲线最大风速的区间（0.5步长）
			double lastWs = NPCBin.get(numBinNPC - 1).get(1);
			int baseLastWs = (int)Math.floor(lastWs);
			double rearBinStartWs = lastWs;
			if(lastWs < baseLastWs + 0.25) {
				rearBinStartWs = baseLastWs;
			}else if(lastWs >= baseLastWs + 0.25 && lastWs < baseLastWs + 0.75) {
				rearBinStartWs = baseLastWs + 0.5;
			}else {
				rearBinStartWs = baseLastWs + 1;
			}
			// 补全待插补的尾部Bin区间风速。
			for(int iBin = 0; iBin < RearBin.size(); iBin++) {
				RearBin.get(iBin).set(0, rearBinStartWs + (iBin + 1)*0.5);
			}
			// % 待插补的尾部Bin区间个数
//			int numIntervals = RearBin.size();
//			for (int iBin = EndBinNoNPC + 1; iBin <= EndBinNoNPC + numIntervals; iBin++) {
//
//				// % 给待插补的尾部Bin区间赋予风速数组
//
//				RearBin.get(iBin - 1 - EndBinNoNPC).set(0, iBin * 0.5);
//			}
		}
//		// % 把尾部Bin区间和经处理的Bin区间合并，完成整个区间拟合。 //?????
		for (int i = 0; i < NPCBin.size(); i++) {
			List<Double> tempList = new ArrayList<Double>();
			tempList.add(NPCBin.get(i).get(1));
			tempList.add(NPCBin.get(i).get(2));
			ReferBin.add(tempList);
		}
		ReferBin.addAll(RearBin);

		// %********************************** 外推功率曲线
		// **************************%
		double numBinTotal = (Math.round(cutOutWS / 0.5) + 1);
		int numReferBin = ReferBin.size();

		if (numBinTotal < numReferBin) {
			List<List<Double>> removeList = new ArrayList<List<Double>>();
			for (double i = numBinTotal + 1; i <= ReferBin.size(); i++) {
				removeList.add(ReferBin.get((int) (i - 1)));
			}
			ReferBin.removeAll(removeList);
		}
		numReferBin = ReferBin.size();
		// % 找到Bin区间最大风速
		double binMaxRefer = Math
				.round(ReferBin.get(numReferBin - 1).get(0) / 0.5);
		// % 找到Bin区间最大功率
		double maxReferPower = ReferBin.get(numReferBin - 1).get(1);

		double gaps = Math.round((cutOutWS - ReferBin.get(numReferBin - 1).get(
				0)) / 0.5) + 1;

		for (int iBin = 1; iBin <= gaps; iBin++) {
			// alter by dulm("<cutOutWs"修改为"<(cutOutWs-0.25)") 如果出现功率曲线最大风速属于切出风速范围，而有小于切出风速时，将会后延出一个多余的区间。
			// 例如：最大风速为12.3，切出风速=12.5时，会后延出一个13风速的区间。但13风速区间已经超出了12.5的切出风速范围。
			if (ReferBin.get(numReferBin + iBin - 2).get(0) < (cutOutWS - 0.25)) {

				List<Double> vList = new ArrayList<Double>();
				vList.add((binMaxRefer + iBin) * 0.5);
				vList.add(maxReferPower);
				ReferBin.add(vList);
			} else
				break;
		}

		return ReferBin;
	}
	/**
	 * 获取最大值
	 * @param list
	 * @param column 列数 0开始
	 * @return
	 */
	public static double getMax(List<List<Double>> list, int column){
		double max = 0.0;
		for(int j = 0; j < list.size(); j++){
			if(list.get(j).get(column) > max){
				max = list.get(j).get(column);
			}
		}
		return max;
	}
	
	public static Map<String, List<List<Double>>> getUpAndDown(List<List<Double>> ReferBin){
		if(ReferBin == null || ReferBin.size() == 0) {
			// 若实际功率曲线没有
			return null;
		}
		List<List<Double>> UpPCBin = new ArrayList<List<Double>>(); //上限 List
		List<List<Double>> DownPCBin = new ArrayList<List<Double>>(); //下限 List
		for(int i = 0; i<ReferBin.size(); i++){
			
			List<Double> tempList = new ArrayList<Double>();
			for(int j = 0; j<ReferBin.get(i).size(); j++){
				double tempNum = ReferBin.get(i).get(j);
				tempList.add(tempNum);
			}
			DownPCBin.add(tempList);
		}
		for(int i = 0; i<ReferBin.size(); i++){
			
			List<Double> tempList = new ArrayList<Double>();
			for(int j = 0; j<ReferBin.get(i).size(); j++){
				double tempNum = ReferBin.get(i).get(j);
				tempList.add(tempNum);
			}
			UpPCBin.add(tempList);
		}
        //%------------- 定义上限PC ---------
        for (int i = 0; i < UpPCBin.size(); i++) {
//        	double X = ReferBin.get(i).get(0);
//			double value = UpPCBin.get(i).get(2 - 1) + (X * 45.561 - 81.963) * wtParam.getUpCoef();
        	// TODO
        	double value = UpPCBin.get(i).get(2 - 1) * 1.4;
			if (value <= 0) {
				UpPCBin.get(i).set(2 - 1, 10.0);
			} else {
				UpPCBin.get(i).set(2 - 1, value);
			}
        }
            
        //%------------- 定义下限PC --------- 
		for (int i = 0; i < DownPCBin.size(); i++) {
			
//			double X = ReferBin.get(i).get(0);
//			double value = (DownPCBin.get(i).get(2 - 1) - (31.454*X - 42.399)) * wtParam.getDownCoef();
			// TODO
			double value = DownPCBin.get(i).get(2 - 1) * 0.8;
			DownPCBin.get(i).set(2 - 1, value);
			// % 前面赋为0
			if(DownPCBin.get(i).get(0) <= 2){
				DownPCBin.get(i).set(2-1, 0.0); 
			}
		}
		
		double max = getMax(DownPCBin, 1);

		int maxIndex = 1;
		for (int i = 0; i < DownPCBin.size(); i++) {
			if(DownPCBin.get(i).get(2-1) == max){
				maxIndex = i;
				break;
			}
		}
		// %----- 优化上限PC和下限PC后面的bin区间 ----- 
		// 只需要获取到第一次出现最大值的索引即可
		if(maxIndex > 0){
			DownPCBin.get(maxIndex - 1).set(1, DownPCBin.get(maxIndex).get(1) * 1.1);
			DownPCBin.get(maxIndex).set(1, DownPCBin.get(maxIndex).get(1) * 1.1);
			int downPCBinSize = DownPCBin.size();
			if(maxIndex + 1 < downPCBinSize) {
				DownPCBin.get(maxIndex + 1).set(1, DownPCBin.get(maxIndex).get(1) * 1.1);
			}
			if(maxIndex + 2 < downPCBinSize) {
				DownPCBin.get(maxIndex + 2).set(1, DownPCBin.get(maxIndex).get(1) * 1.2);
			}
			if(maxIndex + 3 < downPCBinSize) {
				DownPCBin.get(maxIndex + 3).set(1, DownPCBin.get(maxIndex).get(1) * 1.3);
			}
			if(maxIndex + 4 < downPCBinSize) {
				DownPCBin.get(maxIndex + 4).set(1, DownPCBin.get(maxIndex).get(1)*1.4);
			}
		}
		max = getMax(DownPCBin, 1);
		for(int i = maxIndex + 5; i < DownPCBin.size(); i++){
			DownPCBin.get(i).set(2-1, max);
		}
		if (maxIndex > 0) {
			for(int i = maxIndex; i < UpPCBin.size(); i++){
				UpPCBin.get(i).set(2-1, UpPCBin.get(maxIndex-1).get(2-1));
			}
		}
		Map<String, List<List<Double>>> result = new HashMap<String, List<List<Double>>>();
		result.put(Key_UP, UpPCBin);
		result.put(Key_DOWN, DownPCBin);
		return result;
	}
}
