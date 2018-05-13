package com.neu;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.neu.utils.DateUtil;
import com.neu.vo.AvgVO;
import com.neu.vo.FanDataVO;

public class DataQualityCheck implements Serializable{
	
	private static final long serialVersionUID = -2576407132949085136L;
	private static Logger log = Logger.getLogger(DataQualityCheck.class);
	
	public static final String RESULT_DATA = "resultData";
	
	public static final String AVG_MAP = "avgMap";
	
	public JavaRDD<FanDataVO> run(
			JavaRDD<FanDataVO> data,
			final double minTime,
			final double maxTime){
		JavaRDD<FanDataVO> data1 = data.filter(new Function<FanDataVO, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(FanDataVO vo) throws Exception {
				// 剔除时间列为空的数据
				if (StringUtils.isBlank(vo.getTime())) {
					return false;
				}
				double time = Double.parseDouble(vo.getTime());
				// 剔除时间列早于2000年的数据
				if (time < 36526 || time < minTime || time >= maxTime) {
					return false;
				}

				// 剔除超出量程的风速数据
				if (StringUtils.isBlank(vo.getWt_WsAvg())) {
					return false;
				}
				double wt_WsAvg = Double.parseDouble(vo.getWt_WsAvg());
				if(wt_WsAvg == -902) {
					return false;
				}
				if (wt_WsAvg > 12
						|| wt_WsAvg < 3) {
					return false;
				}
				
				if (StringUtils.isBlank(vo.getWt_PowerAvg())) {
					return false;
				}
				// 剔除超出量程的功率数据
				double wt_PowerAvg = Double.parseDouble(vo.getWt_PowerAvg());
				if (wt_PowerAvg < -0.5  * 1500 || wt_PowerAvg > 2 * 1500) {
					return false;
				}
				
				return true;
			}
		});
		
		JavaPairRDD<String, FanDataVO> pairRDD = data1.mapToPair(new PairFunction<FanDataVO, String, FanDataVO>() {

			private static final long serialVersionUID = 5849114701636760170L;

			@Override
			public Tuple2<String, FanDataVO> call(FanDataVO vo) throws Exception {
				String farmId = vo.getFarmNo();
				String fanNo = vo.getFanNo();
				String month = DateUtil.getMonthByDays(vo.getTime());
				String key = farmId + "_" + fanNo + "_" + month;
				return new Tuple2<String, FanDataVO>(key, vo);
			}
		});
		
		
		JavaPairRDD<String, AvgVO> wsAvgRDD = pairRDD.combineByKey(new Function<FanDataVO, AvgVO>() {

			private static final long serialVersionUID = 1670005156808411036L;

			@Override
			public AvgVO call(FanDataVO v1) throws Exception {
				AvgVO avgVO = new AvgVO();
				avgVO.setSum(Double.parseDouble(v1.getWt_WsAvg()));
				avgVO.addNum();
				return avgVO;
			}
		}, new Function2<AvgVO, FanDataVO, AvgVO>() {

			private static final long serialVersionUID = -5515502817705805608L;

			@Override
			public AvgVO call(AvgVO v1, FanDataVO v2) throws Exception {
				v1.addSum(Double.parseDouble(v2.getWt_WsAvg()));
				v1.addNum();
				return v1;
			}
		}, new Function2<AvgVO, AvgVO, AvgVO>() {

			private static final long serialVersionUID = 7877224367040475547L;

			@Override
			public AvgVO call(AvgVO v1, AvgVO v2) throws Exception {
				v1.addSum(v2.getSum());
				v1.addNum(v2.getNum());
				return v1;
			}
		});
		
		// 每台风机每月的平均风速。
		final Map<String, AvgVO> wsAvgMap = wsAvgRDD.collectAsMap();
		
		// 将不符合条件的值替换掉
		JavaRDD<FanDataVO> data2 = data1.map(new Function<FanDataVO, FanDataVO>() {

			private static final long serialVersionUID = -2766412064841322869L;

			@Override
			public FanDataVO call(FanDataVO vo) throws Exception {

				// 剔除超出量程的机舱温度数据
				if (StringUtils.isNotBlank(vo.getWt_T())) {
					double wtT = Double.parseDouble(vo.getWt_T());
					if (wtT < -50 || wtT > 90) {
						String farmId = vo.getFarmNo();
						String fanNo = vo.getFanNo();
						String month = DateUtil.getMonthByDays(vo.getTime());
						String key = farmId + "_" + fanNo + "_" + month;
						double wsAvg = wsAvgMap.get(key).getAvg();
						vo.setWt_T(String.valueOf(wsAvg));
					}
				}
				return vo;
			}
		});
		return data2;
	}
}
