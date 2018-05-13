package com.neu.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.neu.utils.SqlHelper;

public class GPCDao {
	
	private SqlHelper sqlHelper = null;
	
	public GPCDao(){
		sqlHelper = new SqlHelper();
	}
	
	/**
	 * 根据风场ID查询风场下所有机型点保证功率曲线
	 * 
	 * @param farmIDs 多个风场ID
	 * @return 指定风场ID的风场下所有机型对应点GPC。
	 * key=机型，value=功率曲线
	 * @throws SQLException 
	 */
	public Map<String, List<List<Double>>> getGPC(List<String> farmIDs) throws SQLException {
		String placeHolders = sqlHelper.preparePlaceHolders(farmIDs.size());
		String farmIdsStr = Arrays.toString(farmIDs.toArray()).replace("[", "").replace("]", "");
		String sql = "SELECT DISTINCT G.* FROM GPC G WHERE G.WIND_ID IN("+placeHolders+")"+ "ORDER BY G.MACH_MOD,G.WIND_SPEED";
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql, farmIdsStr);
		Map<String, List<List<Double>>> gpcMap = new HashMap<String, List<List<Double>>>();
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			// 将查询出点GPC数据按照风机类型归类。
			String fanType = String.valueOf(row.get("MACH_MOD"));
			String farmID = String.valueOf(row.get("WIND_ID"));
			String key = farmID + "_" + fanType;
			List<List<Double>> gpc = gpcMap.get(key);
			if(gpc == null) {
				gpc = new ArrayList<List<Double>>();
			}
			List<Double> bin = new ArrayList<Double>();
			String windSpeed = String.valueOf(row.get("WIND_SPEED"));// 风速
			String power = String.valueOf(row.get("POWER"));// 功率
			bin.add(Double.parseDouble(windSpeed));
			bin.add(Double.parseDouble(power));
			gpc.add(bin);
			// key=机型，value=保证功率曲线
			gpcMap.put(key, gpc);
		}
		return gpcMap;
	}
	
	
	/**
	 * 获取GPC模板
	 * @param farmIDs 多个风场ID 
	 * 
	 * @return 模板GPC
	 * @throws SQLException 
	 */
	public Map<String, List<List<Double>>> getGPCTemplate(List<String> farmIDs) throws SQLException {
		Map<String, List<List<Double>>> gpcMap = new HashMap<String, List<List<Double>>>();
		String sql = "SELECT * FROM GPC_TEMPLATE G WHERE G.MACH_MOD IN("+sqlHelper.preparePlaceHolders(farmIDs.size())+")"
		+ "ORDER BY G.MACH_MOD,G.WIND_SPEED";
		String farmIdsStr = Arrays.toString(farmIDs.toArray()).replace("[", "").replace("]", "");
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql, farmIdsStr);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			// 将查询出点GPC数据按照风机类型归类。
			String fanType = String.valueOf(row.get("MACH_MOD"));
			List<List<Double>> gpc = gpcMap.get(fanType);
			if(gpc == null) {
				gpc = new ArrayList<List<Double>>();
			}
			List<Double> bin = new ArrayList<Double>();
			String windSpeed = String.valueOf(row.get("WIND_SPEED"));// 风速
			String power = String.valueOf(row.get("POWER"));// 功率
			bin.add(Double.parseDouble(windSpeed));
			bin.add(Double.parseDouble(power));
			gpc.add(bin);
			// key=机型，value=保证功率曲线
			gpcMap.put(fanType, gpc);
		}
		return gpcMap;
	}
	
	/**
	 * 获取相同容量机型的GPC(平均值)
	 * @param type 机型
	 * @return GPC
	 * @throws SQLException 
	 */
	public Map<String, List<List<Double>>> getGPCAvgByCapacity() throws SQLException {
		Map<String, List<List<Double>>> avgGPCMap = new HashMap<String, List<List<Double>>>();
		String sql = "SELECT gt.WIND_SPEED,AVG(gt.POWER) power,ca.capacity"
				+ " FROM GPC_TEMPLATE gt,(SELECT DISTINCT bi.fan_type,bi.capacity FROM BASICINFO bi) ca"
				+ " WHERE gt.MACH_MOD=ca.fan_type"
				+ " GROUP BY gt.WIND_SPEED,ca.capacity"
				+ " ORDER BY ca.capacity,gt.WIND_SPEED";
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			String windSpeed = String.valueOf(row.get("WIND_SPEED"));
			String power = String.valueOf(row.get("power"));
			String capacity = String.valueOf(row.get("capacity"));
			List<List<Double>> avgGPC = avgGPCMap.get(capacity);
			if(avgGPC == null) {
				avgGPC = new ArrayList<List<Double>>();
			}
			List<Double> pcRow = new ArrayList<Double>();
			pcRow.add(Double.parseDouble(windSpeed));
			pcRow.add(Double.parseDouble(power));
			
			avgGPC.add(pcRow);
			avgGPCMap.put(capacity, avgGPC);
		}
		return avgGPCMap;
	}

	public void close() {
		sqlHelper.closeConnection();
	}
	
	public static void main(String[] args) throws SQLException {
		GPCDao dao = new GPCDao();
		List<String> farmIDs = new ArrayList<String>();
		farmIDs.add("141643857");
		Map<String, List<List<Double>>> gpcList = dao.getGPC(farmIDs);
		Set<String> keys = gpcList.keySet();
		for (String key : keys) {
			List<List<Double>> gpc = gpcList.get(key);
			System.out.println(key + "_" +gpc.size());
		} 
//		Map<String, List<List<Double>>> gpcTemplateList = dao.getGPCTemplate(farmIDs);
//		Set<String> keys = gpcTemplateList.keySet();
//		for (String key : keys) {
//			List<List<Double>> gpc = gpcTemplateList.get(key);
//			System.out.println(key + "_" +gpc.size());
//		}
//		long startTime = System.currentTimeMillis();
//		Map<String, List<List<Double>>> gpcAvgMap = dao.getGPCAvgByCapacity();
//		long endTime = System.currentTimeMillis();
//		System.out.println("耗时：" + (endTime - startTime));
	}

}
