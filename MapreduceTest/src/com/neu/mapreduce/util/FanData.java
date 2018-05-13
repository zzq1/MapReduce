package com.neu.mapreduce.util;

public class FanData {

	private String dataSource;// 数据来源
	
	private String fanNo;
	
	private String windSpeed;// 风速
	
	private String time;
	
	private String rs;// 转速
	
	public void getInstance(String row) {
		String[] cols = row.split(",");
		this.dataSource = cols[0];
		this.fanNo = cols[1];
		this.windSpeed = cols[4];
		this.time = cols[2];
		this.rs = cols[6];
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getFanNo() {
		return fanNo;
	}

	public void setFanNo(String fanNo) {
		this.fanNo = fanNo;
	}

	public String getWindSpeed() {
		return windSpeed;
	}

	public void setWindSpeed(String windSpeed) {
		this.windSpeed = windSpeed;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getRs() {
		return rs;
	}

	public void setRs(String rs) {
		this.rs = rs;
	}
	
	
	@Override
	public String toString() {
		return "dataSource=" + this.dataSource
				+ ",fanNo=" + this.fanNo + ",windSpeed=" + this.windSpeed + ",rs=" + this.rs;
	}

	public static void main(String[] args) {
		FanData fanData = new FanData();
		String row = "pi10mr,WT02287,2015-09-01 00:00:00,1,3.54,0,0,214.89,0,40.76,40.64,20.5,26.31,47.05,0,0,0,0,0,0,0,0,-0.52,0.32,9.46539e+06,0,0,0,27";
		fanData.getInstance(row);
		System.out.println(fanData);
	}
}
