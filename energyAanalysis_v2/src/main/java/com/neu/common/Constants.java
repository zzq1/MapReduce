package com.neu.common;

public interface Constants {
	public static final int SLOPE_COLU = 5;// NTF斜率

	public static final int OFFSET_COLU = 6;// NTF偏移量

	public static final int DATE_COL = 1;// 风机日期列号

	public static final int WS_COL = 2;// 风速列号

	public static final int POWER_COL = 3;// 功率列号

	public static final int RS_COL = 4;// 转速列号

	public static final String GPC_WS_COLNAME = "WS";// GPC风速列名

	public static final String GPC_POWER_COLNAME = "POWER";// GPC功率列名

	public static final String FARMPC_WS_COLNAME = "WS";// FarmPC风速列名

	public static final String FARMPC_POWER_COLNAME = "POWER";// FramPC功率列名

	public static final double TEN_MINUTE = 1 * 24 * 6;// 每天的10分钟数据数量

	public static final String FARM_MONTH = "FARM_MONTH";// 每月每台风机的分析结果

	public static final String FARM_MONTH_HIGHT = "FARM_MONTH_HIGHT";// 每月每台风机的高温降熔和停机数据的分析结果

	public static final String FARM_MONTH_STATUS = "FARM_MONTH_STATUS";// 每月每台风机的更具状态划分停运类型的分析结果

	/**
	 * 数据标签：1-通讯中断；2-切入、切出以外；3-限电；4-故障；5-计划检修停运；6-场外受累停运；7-场内受累停运
	 * 
	 * @author pangjb
	 *
	 */
	public interface ComplantTag {
		public static final int TAG_COMMUNICATE_INTERRUPT = 1;// 通讯中断

		public static final int TAG_UP_DOWN_WS = 2;// 切入、切出以外

		public static final int TAG_BROWNOUTS = 3;// 限电

		public static final int TAG_FAULT = 4;// 故障

		public static final int TAG_PLAN_OUTAGE = 5;// 计划检修停运

		public static final int TAG_OUT_INVOKE_OUTAGE = 6;// 场外受累停运

		public static final int TAG_IN_INVOKE_OUTAGE = 7;// 场内受累停运
	}

	/*
	 * 是否有效
	 */
	public static final int INVALID = 1;// 无效

	public static final int VALID = 0;// 有效

	public interface Tag1 {

		public static final int TAG1_NORMAL = 1;// 正常数据

		public static final int TAG1_HIGH_TDERA = 2;// 降容数据

		public static final int TAG1_TURBINE_DOWN = 3;// 停机数据数据

		public static final int TAG1_DOWN_WS = 4;// 小于切入风速

		public static final int TAG1_UP_WS = 5;// 大于切出风速

		public static final int TAG1_COMMUNICATE_INTERRUPT = 6;// 通讯中断数据
	}

	public interface Tag2 {

		public static final int TAG2_HIGH_TDERA = 1;// 高温降容数据

		public static final int TAG2_HIGH_TDOWN = 2;// 高温停机数据
	}

	public interface Tag3 {

		public static final int TAG3_OUT_INVOKE_OUTAGE = 1;// 场外受累停运

		public static final int TAG3_BROWNOUTS = 2;// 限电
	}

	public interface Tag5 {

		public static final int TAG5_NORMAL = 1; // 正常数据

		// public static final int TAG5_PERFORMANCE = 2; //性能
		public static final int TAG5_TDERA = 2; // 降容

		public static final int TAG5_FAULT = 3;// 故障

		public static final int TAG5_IN_INVOKE_OUTAGE = 4;// 场内受累停运

		public static final int TAG5_OUT_INVOKE_OUTAGE = 5;// 场外受累停运

		public static final int TAG5_PLAN_OUTAGE = 6;// 计划检修停运

		public static final int TAG5_BROWNOUTS = 7;// 限电

		public static final int TAG5_UP_DOWN_WS = 8;// 切入、切出以外

		public static final int TAG5_COMMUNICATE_INTERRUPT = 9;// 通讯中断

	}

	public interface FanDataType {

		public static final String NTF_DATA = "NTF";

		public static final String ACCUMULATE_DATA = "ACCUMULATE";
	}
	
	/**
	 * 算法运行数据的时间跨度
	 */
	public interface DataTypeSpan {
		
		public static final int HISTORY = 0;
		
		public static final int DAY = 1;
		
		public static final int MONTH = 2;
		
		public static final int YEAR = 3;
	}

	public static final int ZERO = 0;// tag为空时候的取值

	public static final int LIMIT_COUNT = 4;// 限电边界值

	public static final int METHOD_SELECT = 3;// 数据筛选算法选择

	public static final double TDERA_RATIO = 0.3;// 降容比例

	public static final double BROWNOUTS_RATIO = 0.667;// 限电比例

	public static final int BROWNOUTS_TIME_INTERVAL = 2;// 限电时间隔

	public static final double AEPSIZE_PERCENTAGE = 0.1;// 计算实际功率曲线时排除 AEP 百分比

	public static final double START_BYBEST = 0.05;// 计算实际功率曲线时排除 AEP 百分比

	public static final double END_BYBEST = 0.15;// 计算实际功率曲线时排除 AEP 百分比

	public static final double ROUMONTH_DEFAULT = 1.225;// 默认空气密度

	public static final double HUMIDITY_DEFAULT = 0.5;// 默认湿度
	
	public static final double THEORY_AEP_WS = 6.0;// 理论AEP的平均风速
	
	public static final double CUT_IN_CONFIGURE = 1.0;// 切入风速限值
	
	public static final double LIMIT_CONFIGURE = 5.0;// 限电数据分类风速下限值
	
	public static final double THAEP_WINDSPEED_AVG = 6.0; // 计算理论AEP所用到的平均风速
	
	public static final int NUM_PARITITIONS = 60; // 重新分区后分区个数
	
	/**
	 * 算法运行数据的时间跨度
	 */
	public interface Execution_State {
		
		public static final String READY = "READY";
		
		public static final String RUNNING = "RUNNING";
		
		public static final String FINISH = "FINISH";
		
	}
	/**
	 * 数据标签： 0-历史数据； 1-日常数据；2-月数据、切出以外；3-年数据；4-调试；
	 * 
	 * @author czq
	 *
	 */
	public interface TaskType {
		public static final int HIS = 0;// 历史数据

		public static final int DAY = 1;// 日常数据

		public static final int MONTH = 2;// 月数据

		public static final int YEAR = 3;// 年数据

		public static final int DEBUG = 4;// 调试

	}
	
}
