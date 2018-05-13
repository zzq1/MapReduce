package com.neu;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import com.neu.common.AnalysisException;
import com.neu.utils.CommonUtils;
import com.neu.utils.DateUtil;
import com.neu.vo.FanDataVO;
import com.neu.vo.FarmDicVO;

public class Main {

	private static Logger log = Logger.getLogger(Main.class);
	
	// 读取属性文件
	public static Properties prop = CommonUtils.getAllProperties(Main.class
			.getResourceAsStream("/config.properties"));

	@SuppressWarnings({ "unchecked", "resource" })
	public static void main(String[] args) throws AnalysisException {
		SparkConf conf = new SparkConf();
		conf.setAppName("energyAnalysis");
		conf.setMaster("local[*]");
		SparkContext sc = new SparkContext(conf); 
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		String farmId = args[0];// 风场ID
		String startTime = args[1];
		String endTime = args[2];
		
		InitializeLoad init = new InitializeLoad(Arrays.asList(farmId));
		Map<String, Object> initParam = null;
		// 加载初始化参数。
		try {
			initParam = init.InitialzeParams();
		} catch (Exception e1) {
			//log.error("初始化参数加载失败。风场ID:" + farmID, e1);
			throw new AnalysisException("初始化参数加载失败。风场ID:" + farmId, e1);
		}
		Map<String, FarmDicVO> dicInfo = (Map<String, FarmDicVO>) initParam
				.get(InitializeLoad.INIT_DIC);//台账信息
		final Broadcast<Map<String, FarmDicVO>> signPrefixes = jsc.broadcast(dicInfo);//广播变量台账数据
		Map<String, List<List<Double>>> GPC = (Map<String, List<List<Double>>>) initParam
				.get(InitializeLoad.INIT_GPC);// 保证功率曲线数据。
		log.info("初始化参数结束。" +  farmId);
		log.info("----------------------------------------------------------");
		// 1.加载历史10min数据。
		DataLoad dl = new DataLoad();
//		JavaRDD<FanDataVO> data = dl.loadHiveData(sc, farmId, startTime, endTime, signPrefixes);
		JavaRDD<FanDataVO> data = dl.loadHDFSData(jsc, "/data/tenmindata.csv", signPrefixes);

		// 2.脏数据处理。
		DataQualityCheck check = new DataQualityCheck();
		int startYear = Integer.parseInt(startTime.substring(0, 4));
		int startMonth = Integer.parseInt(startTime.substring(5, 7));
		int endYear = Integer.parseInt(endTime.substring(0, 4));
		int endMonth = Integer.parseInt(endTime.substring(5, 7));
		// 转换开始年月时间为数字（按天计算）
		double minTime = DateUtil.getStartDateNum(startYear, startMonth);
		// 转换结束年月为数字(按天计算)
		double maxTime = DateUtil.getEndDateNum(endYear, endMonth);
		JavaRDD<FanDataVO> checkedData = check.run(data, minTime, maxTime);
		
		ThresholdFilterByWS_Power filterWP = new ThresholdFilterByWS_Power();
		Map<String, Object> filterWPResult = filterWP.run(checkedData, GPC, signPrefixes, 3);
		JavaRDD<FanDataVO> normalData = (JavaRDD<FanDataVO>)filterWPResult.get(ThresholdFilterByWS_Power.KEY_NORMALDATA);
		JavaRDD<FanDataVO> abNormalData = (JavaRDD<FanDataVO>)filterWPResult.get(ThresholdFilterByWS_Power.KEY_ABNORMALDATA);
		Map<String, List<List<Double>>> farmPC = (Map<String,List<List<Double>>>)filterWPResult.get(ThresholdFilterByWS_Power.FARMPC);
	}

}
