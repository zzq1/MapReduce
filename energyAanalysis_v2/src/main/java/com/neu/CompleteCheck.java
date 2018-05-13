package com.neu;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Bin区间完整性检查，并根据需要进行线性插补
 * @author chengzq
 *
 */
public class CompleteCheck implements Serializable{

	private static final long serialVersionUID = 8040472848761741210L;
	private static Logger log = Logger.getLogger(CompleteCheck.class);
	
	public static void main(String[] args) {
	}
	
	/**
	 * Bin区间完整性检查，并根据需要进行线性插补
	 * 
	 * @param Data 测量功率曲线的Bin值
	 * @param binNoColName Bin区间列序号
	 * @param wsColName 风速列序号
	 * @return 完整性检查后Bin区间数据
	 */
	public List<List<Double>> run(List<List<Double>> Data, int BinNO_COLU, int WS_COLU){
		
		int COMPLETEDOTS = 2;
		//剔除风速为0的数据
		for(int i = 0; i < Data.size(); i++){
			List<Double> douList = Data.get(i);
			if(douList.get(WS_COLU-1) != null && douList.get(WS_COLU-1) == 0){
				Data.remove(i);
				i--;
			}
		}
		
		// 找出前面小于3个点的区间
		int nGroup = Data.size();
		int startBinNo = 1;
		int endBinNo = nGroup;
		for(int iGroup = 1; iGroup <= nGroup; iGroup++){
			double binCount = Data.get(iGroup-1).get(BinNO_COLU-1);
			// 如果素所有区间点数均小于COMPLETEDOTS,则startBinNo = iGroup
			if(iGroup == nGroup && binCount < COMPLETEDOTS) {
				startBinNo = iGroup;
			}
			if(Data.get(iGroup-1).get(BinNO_COLU-1) >= COMPLETEDOTS){
				 startBinNo = iGroup;
		         break;
			}
		}
		
		//找出后面小于3个点的区间
		int jGroup = nGroup;
		while (jGroup >= 1){
			double binCount = Data.get(jGroup-1).get(BinNO_COLU-1);
			// 如果所有区间点数均小于COMPLETEDOTS，则endBinNo = 0;
			if(jGroup == 1 && binCount < COMPLETEDOTS) {
				endBinNo = 0;
			}
			if(Data.get(jGroup-1).get(BinNO_COLU-1) >= COMPLETEDOTS){
				endBinNo = jGroup;
		        break;
			}
			jGroup = jGroup - 1;
		}
		
		//提取NTFArray中符合Bin区间完整性条件的点
		List<List<Double>> DataExtract = new ArrayList<List<Double>>();
		for(int i = startBinNo; i <= endBinNo; i++){
			DataExtract.add(Data.get(i-1));
		}
		
		//寻找NTFArray中间部分不满足完整性条件的点，数据置为0（除了Bin区间数目）
		for(int i = 0; i < DataExtract.size(); i++){
			List<Double> douList = DataExtract.get(i);
			if(douList.get(BinNO_COLU-1) < COMPLETEDOTS){
				douList.set(1, 0.0);
				douList.set(2, 0.0);
			}
		}
		
		//获取DataExtract的行数与列数
		int binLength = DataExtract.size();
		int numColu = 0;
		if(DataExtract.size() > 0) {
			numColu = DataExtract.get(0).size();
		}
		
		//遍历所有区间，并填补空区间
		int intervalBins = 0;
		for(int iBin = 1; iBin <= binLength; iBin++){
			if(DataExtract.get(iBin-1).get(BinNO_COLU-1) < COMPLETEDOTS){
				int num_NullBin = 1;
				
				for(int jBin = iBin+1; jBin <= binLength; jBin++){
					intervalBins = jBin - iBin;
					//往后进行查找，直到发现非空区间
					if(DataExtract.get(jBin-1).get(BinNO_COLU-1) >= COMPLETEDOTS){
						if (intervalBins>3){
							break;
						}
						//对空区间差值
						int endNotNullBinNo = iBin + num_NullBin ;
						//对numColu-2个列的插值循环
						for(int iColu = 1; iColu <= numColu; iColu++){
							if(iColu != BinNO_COLU && iColu != WS_COLU){
								double y2 = DataExtract.get(endNotNullBinNo-1).get(iColu-1);
								double y1 = DataExtract.get(iBin-1-1).get(iColu-1);
								double x2 = DataExtract.get(endNotNullBinNo-1).get(WS_COLU-1);
								double x1 = DataExtract.get(iBin-1-1).get(WS_COLU-1);
								
								double k = (y2-y1)/(x2-x1);
								double b = y2 - k*x2;
								
								for(int mBin = iBin; mBin <= endNotNullBinNo-1; mBin++){
									double value_1 = Math.round(DataExtract.get(mBin-1-1).get(WS_COLU-1)/0.5+1)*0.5;
									DataExtract.get(mBin-1).set(WS_COLU-1, value_1);

									double value_2 = k * DataExtract.get(mBin-1).get(WS_COLU-1)+b;
									DataExtract.get(mBin-1).set(iColu-1, value_2);
								}
							}
						}
						break;//插值完毕，跳出插值程序
					} else {//发现非空区间，则非空区间数目加1
						num_NullBin = num_NullBin + 1;
					}
				}
				if (intervalBins>3)break;
			}
		}
		
		return DataExtract;
	}
	
	/**
	 * 测试生成数据
	 * @param row
	 * @param column
	 * @return
	 */
	private static List<List<Double>> generateTestData(int row, int column){
		List<List<Double>> result = new ArrayList<List<Double>>();
		for(int i = 0; i < row; i++){
			List<Double> temp = new ArrayList<Double>();
			for(int j = 0; j < column; j++){
				temp.add((Math.random() * 35 + 1));
			}
			result.add(temp);
		}
		
		return result;
	}
}
