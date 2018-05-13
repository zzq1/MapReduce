package com.neu;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import com.neu.common.Constants;
import com.neu.utils.CommonUtils;
import com.neu.utils.DateUtil;
import com.neu.vo.FanDataVO;
import com.neu.vo.FarmDicVO;

public class ThresholdFilterByWS_PowerFunc {
	
	/**
	 * 根据上限功率曲线和下限功率曲线筛选出正常数据和异常数据
	 * 
	 * @author kangcc
	 * 
	 */
	public static class func implements Function<FanDataVO, Boolean> {

		private static final long serialVersionUID = 1L;
		Map<String, List<List<Double>>> UpPCBin = null;
		Map<String, List<List<Double>>> DownPCBin = null;
		Map<String, List<List<Double>>> ReferBin = null;
		int iTime;
		private String filterType;
		boolean isDay = false;
		Map<String, FarmDicVO> formDicInfo = null;

		/**
		 * 根据上限功率曲线和下限功率曲线筛选出正常数据和异常数据
		 * 
		 * @param UpPCBin
		 *            上限功率曲线(key=风场ID_机型_年月),当iTime=1时使用。
		 * @param DownPCBin
		 *            下限功率曲线(key=风场ID_机型_年月),当iTime=1时使用。
		 * @param iTime
		 *            迭代次数
		 * @param ReferBin
		 * 			     根据正常数据计算的实际功率曲线（已包含了上限下限值）
		 * @param signPrefixes 风场台账数据(key=风机编号，value=台账信息)
		 * @param filterType 要返回的数据集类型(正常数据/异常数据)
		 */
		public func(Map<String, List<List<Double>>> UpPCBin,Map<String, List<List<Double>>> DownPCBin,
				int iTime, Map<String, List<List<Double>>> ReferBin,
				Broadcast<Map<String, FarmDicVO>> signPrefixes, 
				String filterType,
				boolean isDay) {
			this.UpPCBin = UpPCBin;
			this.DownPCBin = DownPCBin;
			this.iTime = iTime;
			this.ReferBin = ReferBin;
			this.filterType = filterType;
			this.isDay = isDay;
			formDicInfo = signPrefixes.getValue();
		}

		@Override
		public Boolean call(FanDataVO v1) throws Exception {
			double ws = Double.parseDouble(v1.getWt_WsAvg());
			double wsFloor = Math.floor(ws);
			String farmNo = v1.getFarmNo();// 风场ID
			String fanNo = v1.getFanNo();// 风机编号
			String fanType = formDicInfo.get(fanNo).getFanType();
			String yearMonth  = DateUtil.getMonthByDays(v1.getTime());
			int section = 0;
			String  key  = null;
			// 寻找该条数据所在功率曲线的区间。
			if(ws >= wsFloor-0.25 && ws < wsFloor+0.25) {
				section = (int)(wsFloor/0.5);
			}else if(ws >= wsFloor+0.25 && ws < wsFloor + 0.75) {
				section = (int)(wsFloor/0.5 + 1);
			}else if(ws >= wsFloor + 0.75 && ws < wsFloor + 1.25) {
				section = (int)(wsFloor/0.5 + 2);
			}
			double upPow = 0.0d;
			double downPow = 0.0d;
			boolean bool = false;
			if (iTime > 1) {
				if (isDay) { // 如果为过滤当天数据时则按照前取前10天的规则去风速功率曲线 key:风场_风机_年月_段
					yearMonth = CommonUtils.getTenDayKey(v1.getTime());
				}
				key  =  farmNo + "_" + fanNo+"_"+ yearMonth;
				//取出数据所在月份的功率曲线
				List<List<Double>> referBin = ReferBin.get(key);//key=风场id + 风机编号 + 年月，
				//可能出现某个风场某台风机某个月的数据都为abNormalData的情况，此时referBin得到的值是null,直接返回false
				if (null!=referBin && referBin.size()>0){//
					// 取出该风速所在功率曲线的区间的上下限功率
					upPow = referBin.get(section).get(2);
					downPow = referBin.get(section).get(3);
				}else{
					v1.setTag1(Constants.Tag1.TAG1_HIGH_TDERA);
					if(filterType.equals(ThresholdFilterByWS_Power.KEY_NORMALDATA)) {
						return false;
					}else if(filterType.equals(ThresholdFilterByWS_Power.KEY_ABNORMALDATA)) {
						return true;
					}
				}
				
			} else {
				key  =  farmNo + "_" + fanType;
				// 取出该风速所在功率曲线的区间的上下限功率
				List<List<Double>> upPC = UpPCBin.get(key);
				List<List<Double>> downPC = DownPCBin.get(key);
				// 如果10min的风速超出了上下限功率曲线的最大风速，则使用功率曲线的最后一个点。
				if(section >= upPC.size()) {
					upPow = UpPCBin.get(key).get(upPC.size() - 1).get(1);
				}else {
					upPow = UpPCBin.get(key).get(section).get(1);
				}
				if(section >= downPC.size()) {
					downPow = DownPCBin.get(key).get(downPC.size() - 1).get(1);
				}else {
					downPow = DownPCBin.get(key).get(section).get(1);
				}
			}
			// 功率
			double pow = Double.parseDouble(v1.getWt_PowerAvg());
			if (filterType.equals(ThresholdFilterByWS_Power.KEY_NORMALDATA)) {// 如果要过滤出正常数据
				// 数据的功率值在上下限之间，则认为是正常数据，返回true，否则为异常数据，返回false。
				if(pow > downPow && pow < upPow) {
					v1.setTag1(Constants.Tag1.TAG1_NORMAL);
					bool = true;
				}else {
					v1.setTag1(Constants.Tag1.TAG1_HIGH_TDERA);
					bool = false;
				}
			}
			if (filterType.equals(ThresholdFilterByWS_Power.KEY_ABNORMALDATA)) {// 如果要过滤出异常数据
				// 数据的功率值在上下限之间，则认为是正常数据，返回false，否则为异常数据，返回true。
				if(pow > downPow && pow < upPow) {
					v1.setTag1(Constants.Tag1.TAG1_NORMAL);
					bool = false;
				}else {
					v1.setTag1(Constants.Tag1.TAG1_HIGH_TDERA);
					bool = true;
				}
			}
			return bool;
		}
	}
}
