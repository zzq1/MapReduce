package com.neu.vo;

import java.io.Serializable;
import java.util.List;

import com.neu.utils.DateUtil;

public class FanDataVO implements Serializable, Comparable<FanDataVO>{

	private static final long serialVersionUID = -4380059523062142051L;

	/**
	 * 指标时间
	 */
	private String Time;
	
	/**
	 * 指标下一个时间点
	 */
	private String Time2;
	
	/**
	 * 年
	 */
	private int year;
	
	/**
	 * 月
	 */
	private int month;
	
	/**
	 * 日
	 */
	private int day;
	
	/**
	 * 风场编号
	 */
	private String FarmNo;
	
	/**
	 * 风机编号
	 */
	private String FanNo;
	
	/**
	 * 数据来源
	 */
	private String dataSource;
	
	/**
	 * 风机风速
	 */
	private String wt_WsAvg;
	
	/**
	 * 风机功率
	 */
	private String wt_PowerAvg;
	
	/**
	 * 风轮转速
	 */
	private String rs;
	
	/**
	 * 测风塔温度
	 */
	private String mast_T;
	
	/**
	 * 测风塔湿度
	 */
	private String humidity;
	
	/**
	 * 叶片角度
	 */
	private String bladeAngle;
	
	/**
	 * 测风塔气压
	 */
	private String mast_Pressure;
	
	/**
	 * 风机温度
	 */
	private String wt_T;
	
	/**
	 * 月空气密度
	 */
	private Double rouMonth;
	
	/**
	 * 月平均空气密度
	 */
	private AvgVO rouMonthAvg;
	
	/**
	 * 风机状态
	 */
	private String wtStatus;

	/**
	 * 计数
	 */
	private double count;
	
	/**
	 * 齿轮箱油温
	 */
	private double boxOilT; 
	
	/**
	 * 齿轮箱轴承温度
	 */
	private double boxBearingT; 
	
	/**
	 * 发电机绕组温度
	 */
	private double rotorGroupT;
	
	/**
	 * 驱动方向发电机轴承温度
	 */
	private double xRotorBearingT; 
	
	/**
	 * 非驱动方向发电机轴承温度
	 */
	private double yRotorBearingT; 
	
	/**
	 * 发电机定子温度
	 */
	private double rotorFixT;
	
	/**
	 * 发电机侧IGBT温度
	 */
	private double rotorIGBT; 
	
	/**
	 * 环境温度
	 */
	private double environT;
	
	/**
	 * 当月所有10min数据
	 */
	private List<FanDataVO> monthFanList;
	
	private String key;
	
	/**
	 * 数据类型(NTF/accumulate)
	 */
	private String dataType;
	
	/**
	 * 和其他数据的功率差值的平均值
	 */
	private double diffPowMean;
	
	/**
	 * 功率上100范围的数据点个数
	 */
	private int upPowPoint;
	
	/**
	 * 功率下100范围的数据点个数
	 */
	private int downPowPoint;
	
	/**
	 * 方差分析值
	 */
	private double anovaStd;
	
	/**
	 * 标准差
	 */
	private double std;
	
	/**
	 *NTF 理论发电量
	 */
	private double NTFTheoryGeneration;
	/**
	 * NTF 损失电量
	 */
	private double NTFGenerationLoss;
	
	/**
	 * NTF 实际应发电量
	 */
	private double NTFRealGenneration;
	
	/**
	 * 最佳功率曲线理论发电量
	 */
	private double BestTheoryGeneration;
	/**
	 * 最佳损失电量
	 */
	private double BestGenerationLoss;
	
	/**
	 * 最佳 实际应发电量
	 */
	private double BestRealGeneration;
	
	/**
	 * 实际功率曲线理论发电量
	 */
	private double RealTheoryGeneration;
	/**
	 * 实际功率曲线损失电量
	 */
	private double RealGenerationLoss;
	
	/**
	 * 
	 */
	private String highTDeratdTye;
	
	/**
	 * 风机类型
	 */
	private String fanType;
	
	/**
	 * 区域公司
	 */
	private String province;
	
	/**
	 * 风场名称
	 */
	private String farm_name;
	
	/**
	 * 风机厂家
	 */
	private String factory;
	
	/**
	 * 发电机转速
	 */
	private String rotorRS;
	
	/**
	 * 叶轮轴承温度
	 */
	private String wheelBearingT;
	
	/**
	 * 累计发电量
	 */
	private String sumG;
	
	/**
	 * 转矩
	 */
	private String torque;
	
	/**
	 * 机舱位置
	 */
	private String tbPosition;
	
	/**
	 * 对风角度
	 */
	private String yaw;
	
	/**
	 * tag1标签，用来标识正常、降容、停机、小于切入风速、大于切出风速的数据
	 */
	private int tag1;
	
	/**
	 * tag2标签，用来表示高温降容、高温停机的数据。
	 */
	private int tag2;
	
	/**
	 * tag3标签，用来标识场外受累、限电的数据。
	 */
	private int tag3;
	
	/**
	 * tag4标签，用来标识通讯中断、切入切出以外、限电、故障、计划检修停运、场外受累停运、场内受累停运。
	 * 根据异常申诉表得出的状态分类。
	 */
	private int tag4;
	/**
	 * tag5标签,根据前四个标签得出的最终结果,把数据标识为正常、性能、故障、场内、场外、计划、限电、切入切出、通讯中断。
	 */
	private int tag5;
	
	private boolean flag = false;
	
	public boolean isFlag() {
		return flag;
	}

	public void setFlag(boolean flag) {
		this.flag = flag;
	}

	public String getFarmNo() {
		return FarmNo;
	}

	public void setFarmNo(String farmNo) {
		FarmNo = farmNo;
	}

	public String getFanNo() {
		return FanNo;
	}

	public void setFanNo(String fanNo) {
		FanNo = fanNo;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public double getCount() {
		return count;
	}

	public void setCount(double count) {
		this.count = count;
	}
	
	public String getTime() {
		return Time;
	}

	public void setTime(String time) {
		Time = time;
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
	
	public String getRs() {
		return rs;
	}

	public void setRs(String rs) {
		this.rs = rs;
	}
	
	public String getMast_T() {
		return mast_T;
	}

	public void setMast_T(String mast_T) {
		this.mast_T = mast_T;
	}

	public String getHumidity() {
		return humidity;
	}

	public void setHumidity(String humidity) {
		this.humidity = humidity;
	}

	public String getMast_Pressure() {
		return mast_Pressure;
	}

	public void setMast_Pressure(String mast_Pressure) {
		this.mast_Pressure = mast_Pressure;
	}

	public String getRotorRS() {
		return rotorRS;
	}

	public void setRotorRS(String rotorRS) {
		this.rotorRS = rotorRS;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((FanNo == null) ? 0 : FanNo.hashCode());
		result = prime * result + ((FarmNo == null) ? 0 : FarmNo.hashCode());
		result = prime * result + ((Time == null) ? 0 : Time.hashCode());
		return result;
	}

	public String getWt_T() {
		return wt_T;
	}

	public void setWt_T(String wt_T) {
		this.wt_T = wt_T;
	}

	public Double getRouMonth() {
		return rouMonth;
	}

	public void setRouMonth(Double rouMonth) {
		this.rouMonth = rouMonth;
	}

	public AvgVO getRouMonthAvg() {
		return rouMonthAvg;
	}

	public void setRouMonthAvg(AvgVO rouMonthAvg) {
		this.rouMonthAvg = rouMonthAvg;
	}

	public String getWtStatus() {
		return wtStatus;
	}

	public void setWtStatus(String wtStatus) {
		this.wtStatus = wtStatus;
	}
	
	public String getTime2() {
		return Time2;
	}

	public void setTime2(String time2) {
		Time2 = time2;
	}
	
	public double getBoxOilT() {
		return boxOilT;
	}

	public void setBoxOilT(double boxOilT) {
		this.boxOilT = boxOilT;
	}

	public double getBoxBearingT() {
		return boxBearingT;
	}

	public void setBoxBearingT(double boxBearingT) {
		this.boxBearingT = boxBearingT;
	}

	public double getRotorGroupT() {
		return rotorGroupT;
	}

	public void setRotorGroupT(double rotorGroupT) {
		this.rotorGroupT = rotorGroupT;
	}

	public double getxRotorBearingT() {
		return xRotorBearingT;
	}

	public void setxRotorBearingT(double xRotorBearingT) {
		this.xRotorBearingT = xRotorBearingT;
	}

	public double getyRotorBearingT() {
		return yRotorBearingT;
	}

	public void setyRotorBearingT(double yRotorBearingT) {
		this.yRotorBearingT = yRotorBearingT;
	}

	public double getRotorIGBT() {
		return rotorIGBT;
	}

	public void setRotorIGBT(double rotorIGBT) {
		this.rotorIGBT = rotorIGBT;
	}

	public double getEnvironT() {
		return environT;
	}

	public void setEnvironT(double environT) {
		this.environT = environT;
	}

	public List<FanDataVO> getMonthFanList() {
		return monthFanList;
	}

	public void setMonthFanList(List<FanDataVO> monthFanList) {
		this.monthFanList = monthFanList;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public double getDiffPowMean() {
		return diffPowMean;
	}

	public void setDiffPowMean(double diffPowMean) {
		this.diffPowMean = diffPowMean;
	}



	public int getUpPowPoint() {
		return upPowPoint;
	}

	public void setUpPowPoint(int upPowPoint) {
		this.upPowPoint = upPowPoint;
	}

	public int getDownPowPoint() {
		return downPowPoint;
	}

	public void setDownPowPoint(int downPowPoint) {
		this.downPowPoint = downPowPoint;
	}

	public double getAnovaStd() {
		return anovaStd;
	}

	public void setAnovaStd(double anovaStd) {
		this.anovaStd = anovaStd;
	}

	public double getStd() {
		return std;
	}

	public void setStd(double std) {
		this.std = std;
	}

	public String getHighTDeratdTye() {
		return highTDeratdTye;
	}

	public void setHighTDeratdTye(String highTDeratdTye) {
		this.highTDeratdTye = highTDeratdTye;
	}

	public int getTag1() {
		return tag1;
	}

	public void setTag1(int tag1) {
		this.tag1 = tag1;
	}

	public int getTag2() {
		return tag2;
	}

	public void setTag2(int tag2) {
		this.tag2 = tag2;
	}

	public int getTag3() {
		return tag3;
	}

	public void setTag3(int tag3) {
		this.tag3 = tag3;
	}

	public int getTag4() {
		return tag4;
	}

	public void setTag4(int tag4) {
		this.tag4 = tag4;
	}

	public int getTag5() {
		return tag5;
	}

	public void setTag5(int tag5) {
		this.tag5 = tag5;
	}

	public double getNTFTheoryGeneration() {
		return NTFTheoryGeneration;
	}

	public void setNTFTheoryGeneration(double nTFTheoryGeneration) {
		NTFTheoryGeneration = nTFTheoryGeneration;
	}

	public double getNTFGenerationLoss() {
		return NTFGenerationLoss;
	}

	public void setNTFGenerationLoss(double nTFGenerationLoss) {
		NTFGenerationLoss = nTFGenerationLoss;
	}

	public double getBestTheoryGeneration() {
		return BestTheoryGeneration;
	}

	public void setBestTheoryGeneration(double bestTheoryGeneration) {
		BestTheoryGeneration = bestTheoryGeneration;
	}

	public double getBestGenerationLoss() {
		return BestGenerationLoss;
	}

	public void setBestGenerationLoss(double bestGenerationLoss) {
		BestGenerationLoss = bestGenerationLoss;
	}

	public double getRealTheoryGeneration() {
		return RealTheoryGeneration;
	}

	public void setRealTheoryGeneration(double realTheoryGeneration) {
		RealTheoryGeneration = realTheoryGeneration;
	}

	public double getRealGenerationLoss() {
		return RealGenerationLoss;
	}

	public void setRealGenerationLoss(double realGenerationLoss) {
		RealGenerationLoss = realGenerationLoss;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}
	
	public String getFanType() {
		return fanType;
	}

	public void setFanType(String fanType) {
		this.fanType = fanType;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getFarm_name() {
		return farm_name;
	}

	public void setFarm_name(String farm_name) {
		this.farm_name = farm_name;
	}

	public String getFactory() {
		return factory;
	}

	public void setFactory(String factory) {
		this.factory = factory;
	}

	public String getBladeAngle() {
		return bladeAngle;
	}

	public void setBladeAngle(String bladeAngle) {
		this.bladeAngle = bladeAngle;
	}

	public double getRotorFixT() {
		return rotorFixT;
	}

	public void setRotorFixT(double rotorFixT) {
		this.rotorFixT = rotorFixT;
	}

	public String getWheelBearingT() {
		return wheelBearingT;
	}

	public void setWheelBearingT(String wheelBearingT) {
		this.wheelBearingT = wheelBearingT;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getSumG() {
		return sumG;
	}

	public void setSumG(String sumG) {
		this.sumG = sumG;
	}

	public String getTorque() {
		return torque;
	}

	public void setTorque(String torque) {
		this.torque = torque;
	}

	public String getTbPosition() {
		return tbPosition;
	}

	public void setTbPosition(String tbPosition) {
		this.tbPosition = tbPosition;
	}

	public String getYaw() {
		return yaw;
	}

	public void setYaw(String yaw) {
		this.yaw = yaw;
	}

	public double getNTFRealGenneration() {
		return NTFRealGenneration;
	}

	public void setNTFRealGenneration(double nTFRealGenneration) {
		NTFRealGenneration = nTFRealGenneration;
	}

	public double getBestRealGeneration() {
		return BestRealGeneration;
	}

	public void setBestRealGeneration(double bestRealGeneration) {
		BestRealGeneration = bestRealGeneration;
	}

	@Override
	public String toString() {
		return FarmNo + "," // 风场ID
				+ province + "," // 区域
				+ farm_name + "," // 风场名称
				+ factory + "," // 厂家
				+ fanType + "," // 机型
				+ FanNo + "," // 风机名称
				+ DateUtil.getDateByDou(Double.parseDouble(Time)) + "," // 时间
				+ wt_WsAvg + "," // 风速
				+ wt_PowerAvg + "," // 功率
				+ rotorRS + "," // 电机转速
				+ boxOilT + "," // 齿轮箱油温
				+ rs + "," // 桨叶转速
				+ boxBearingT + "," // 齿轮箱轴承温度
				+ environT + "," // 环境温度
				+ wt_T + "," // 机舱温度
				+ rotorGroupT + "," // 发电机绕组温度
				+ tag1 + "," // 标签1(1-正常;2-降容数据;3-停机数据数据;4-小于切入风速;5-大于切出风速;6-通讯中断数据)
				+ tag2 + "," // 标签2(1-高温降容;2-高温停机;)
				+ tag3 + "," // 标签3(1-场外受累停运;2-限电;)
				+ tag4 + "," // 标签4(1-通讯中断;2-切入、切出以外;3-限电;4-故障;5-计划检修停运;6-场外受累停运;7-场内受累停运;)
				+ tag5 + "," // 标签5(1-正常数据;2-降容;3-故障;4-场内受累停运;5-场外受累停运;6-计划检修停运;7-限电;8-切入、切出以外;9-通讯中断;)
				+ bladeAngle + "," // 叶片角度
				+ dataType; // 计算类别(NTF-NTF法，ACCUMULATE-累加法)
		//TODO 因为当前没有叶片角度的指标值（为空）所以当将其放在最后一位时可能会出现用Excel打开.csv文件是最后一列丢失的情况
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FanDataVO other = (FanDataVO) obj;
		if (FanNo == null) {
			if (other.FanNo != null)
				return false;
		} else if (!FanNo.equals(other.FanNo))
			return false;
		if (FarmNo == null) {
			if (other.FarmNo != null)
				return false;
		} else if (!FarmNo.equals(other.FarmNo))
			return false;
		if (Time == null) {
			if (other.Time != null)
				return false;
		} else if (!Time.equals(other.Time))
			return false;
		return true;
	}

	public int compareTo(FanDataVO o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
