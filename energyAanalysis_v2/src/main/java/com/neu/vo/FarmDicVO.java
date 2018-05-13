package com.neu.vo;

import java.io.Serializable;

public class FarmDicVO implements Serializable{

	private static final long serialVersionUID = 1L;

	/**
	 * 风机编号
	 */
	private String fanNo;
	
	/**
	 * 区域
	 */
	private String area;
	
	/**
	 * 项目公司
	 */
	private String project;
	
	/**
	 * 风场名称
	 */
	private String farmName;
	
	/**
	 * 风场简称
	 */
	private String farmShortName;
	
	/**
	 * 风场编号
	 */
	private String farmNo;
	
	/**
	 * 风场接入时间
	 */
	private String farmJoinTime;
	
	/**
	 * 风机名称
	 */
	private String fanName;
	
	/**
	 * 风机类型
	 */
	private String fanType;
	
	/**
	 * 机组容量
	 */
	private String fanCapacity;
	/**
	 * 风机厂家
	 */
	private String fanFactory;
	
	/**
	 * 海拔高度
	 */
	private String altitude;
	
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

	public String getFarmName() {
		return farmName;
	}

	public void setFarmName(String farmName) {
		this.farmName = farmName;
	}

	public String getFarmShortName() {
		return farmShortName;
	}

	public void setFarmShortName(String farmShortName) {
		this.farmShortName = farmShortName;
	}

	public String getFarmNo() {
		return farmNo;
	}

	public void setFarmNo(String farmNo) {
		this.farmNo = farmNo;
	}

	public String getFarmJoinTime() {
		return farmJoinTime;
	}

	public void setFarmJoinTime(String farmJoinTime) {
		this.farmJoinTime = farmJoinTime;
	}

	public String getFanName() {
		return fanName;
	}

	public void setFanName(String fanName) {
		this.fanName = fanName;
	}

	public String getFanType() {
		return fanType;
	}

	public void setFanType(String fanType) {
		this.fanType = fanType;
	}

	public String getFanCapacity() {
		return fanCapacity;
	}

	public void setFanCapacity(String fanCapacity) {
		this.fanCapacity = fanCapacity;
	}

	public String getFanFactory() {
		return fanFactory;
	}

	public void setFanFactory(String fanFactory) {
		this.fanFactory = fanFactory;
	}
	
	public String getAltitude() {
		return altitude;
	}

	public void setAltitude(String altitude) {
		this.altitude = altitude;
	}

	@Override
	public String toString() {
		return "FarmDicVO [fanNo=" + fanNo + ", area=" + area + ", project="
				+ project + ", farmName=" + farmName + ", farmShortName="
				+ farmShortName + ", farmNo=" + farmNo + ", farmJoinTime="
				+ farmJoinTime + ", fanName=" + fanName + ", fanType="
				+ fanType + ", fanCapacity=" + fanCapacity + "]";
	}
	
	
}
