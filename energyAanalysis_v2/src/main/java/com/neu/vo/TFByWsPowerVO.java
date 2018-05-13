package com.neu.vo;

import java.io.Serializable;
import java.util.List;

public class TFByWsPowerVO implements Serializable{

	private static final long serialVersionUID = -4380059523062142051L;

	/**
	 * 风机风速
	 */
	private String wt_WsAvg;
	
	private double wt_WsAvgSum;
	
	/**
	 * 风机功率
	 */
	private String wt_PowerAvg;
	
	private double wt_PowerAvgSum;

	/**
	 * 计数
	 */
	private double count;
	
	/**
	 * 同一类的10min数据
	 */
	private List<FanDataVO> dataList;
	
	/**
	 * 中位数（功率）
	 */
	private double medianPower;
	
	/**
	 * 中位数对应的风速
	 */
	private double medianWs;
	
	public double getCount() {
		return count;
	}

	public void setCount(double count) {
		this.count = count;
	}

	public String getWt_WsAvg() {
		return wt_WsAvg;
	}

	public void setWt_WsAvg(String wt_WsAvg) {
		this.wt_WsAvg = wt_WsAvg;
	}
	
	public String getWt_PowerAvg() {
		return wt_PowerAvg;
	}

	public void setWt_PowerAvg(String wt_PowerAvg) {
		this.wt_PowerAvg = wt_PowerAvg;
	}

	public double getWt_WsAvgSum() {
		return wt_WsAvgSum;
	}

	public void setWt_WsAvgSum(double wt_WsAvgSum) {
		this.wt_WsAvgSum = wt_WsAvgSum;
	}

	public double getWt_PowerAvgSum() {
		return wt_PowerAvgSum;
	}

	public void setWt_PowerAvgSum(double wt_PowerAvgSum) {
		this.wt_PowerAvgSum = wt_PowerAvgSum;
	}

	public List<FanDataVO> getDataList() {
		return dataList;
	}

	public void setDataList(List<FanDataVO> dataList) {
		this.dataList = dataList;
	}

	public double getMedianPower() {
		return medianPower;
	}

	public void setMedianPower(double medianPower) {
		this.medianPower = medianPower;
	}

	public double getMedianWs() {
		return medianWs;
	}

	public void setMedianWs(double medianWs) {
		this.medianWs = medianWs;
	}
//	@Override
//	public String toString() {
//		return "FanDataVO [Time=" + Time + ", Time2=" + Time2 + ", wt_WsAvg="
//				+ wt_WsAvg + ", wt_PowerAvg=" + wt_PowerAvg + ", rs=" + rs
//				+ ", mast_T=" + mast_T + ", wt_T=" + wt_T + ", humidity="
//				+ humidity + ", mast_Pressure=" + mast_Pressure + ", wtStatus="
//				+ wtStatus + "]";
//	}
}
