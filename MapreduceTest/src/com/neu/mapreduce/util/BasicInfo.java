package com.neu.mapreduce.util;

public class BasicInfo {

	private String fanNo;
	
	private String area;
	
	private String project;
	
	private String fanName;
	
	private String shortName;
	
	private String farmId;
	
	private String farmName;
	
	private String fanType;
	
	public void getInfo(String row) {
		String[] cols = row.split(",");
		this.fanNo = cols[1];
		this.area = cols[2];
		this.project = cols[3];
		this.farmName = cols[4];
		this.shortName = cols[5];
		this.farmId = cols[7];
		this.fanName = cols[10];
		this.fanType = cols[11];
	}

	public String getFanNo() {
		return fanNo;
	}

	public void setFanNo(String fanNo) {
		this.fanNo = fanNo;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public String getFanName() {
		return fanName;
	}

	public void setFanName(String fanName) {
		this.fanName = fanName;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public String getFarmId() {
		return farmId;
	}

	public void setFarmId(String farmId) {
		this.farmId = farmId;
	}

	public String getFarmName() {
		return farmName;
	}

	public void setFarmName(String farmName) {
		this.farmName = farmName;
	}

	public String getFanType() {
		return fanType;
	}

	public void setFanType(String fanType) {
		this.fanType = fanType;
	}
	
	
	
	@Override
	public String toString() {
		return "BasicInfo [fanNo=" + fanNo + ", area=" + area + ", project="
				+ project + ", fanName=" + fanName + ", shortName=" + shortName
				+ ", farmId=" + farmId + ", farmName=" + farmName
				+ ", fanType=" + fanType + "]";
	}

	public static void main(String[] args) {
		BasicInfo bi = new BasicInfo();
		String row = "富饶山风电场FR1-43,WT02287,辽宁,沈阳龙源雄亚风力发电有限公司,富饶山风电场,富饶山,辽宁,1,2013/1/1,WT02287,FR1-43,GW50-750,750,金风,709,0,0,0,0";
		bi.getInfo(row);
		System.out.println(bi);
	}
	
}
