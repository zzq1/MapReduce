package com.neu.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.neu.utils.SqlHelper;
import com.neu.vo.FarmDicVO;

/**
 * 台账数据表Dao
 * @author DLM
 *
 */
public class BasicInfoDao {

	private SqlHelper sqlHelper;
	
	public BasicInfoDao(){
		sqlHelper = new SqlHelper();
	}
	

	public Map<String, FarmDicVO> getBasicInfoMap() throws SQLException{
		Map<String, FarmDicVO> basicInfoMap = new HashMap<String, FarmDicVO>();
		String sql = "SELECT * FROM BASICINFO BI";
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			String fanNo = String.valueOf(row.get("fan_no"));// 风机编号
			String area = String.valueOf(row.get("region"));// 区域
			String farmName = String.valueOf(row.get("farm_name"));// 风场名称
			String farmShortName = String.valueOf(row.get("farm_nickname"));// 风场简称
			String farmID = String.valueOf(row.get("farm_id"));// 风场ID
			String fanName = String.valueOf(row.get("fan_name"));// 风机名称
			String fanType = String.valueOf(row.get("fan_type"));// 风机类型
			String fanCapacity = String.valueOf(row.get("capacity"));// 机组容量
			String fanFactory = String.valueOf(row.get("factory"));// 风机厂家
			String altitude = row.get("altitude") != null?String.valueOf(row.get("altitude")):null;// 海拔高度
			FarmDicVO farmDicVO = new FarmDicVO();
			farmDicVO.setFanNo(fanNo);
			farmDicVO.setArea(area);
			farmDicVO.setFarmName(farmName);
			farmDicVO.setFarmShortName(farmShortName);
			farmDicVO.setFarmNo(farmID);
			farmDicVO.setFanName(fanName);
			farmDicVO.setFanType(fanType);
			farmDicVO.setFanCapacity(fanCapacity);
			farmDicVO.setFanFactory(fanFactory);
			farmDicVO.setAltitude(altitude);
			basicInfoMap.put(fanNo, farmDicVO);
		}
		return basicInfoMap;
	}
	/**
	 * 查询台账数据
	 * @param farmIds 多个风场ID
	 * @return 台账数据（key=风机编号，value=台账数据）
	 * @throws SQLException 
	 */
	public Map<String, FarmDicVO> getBasicInfoMap(List<String> farmIds) throws SQLException{
		Map<String, FarmDicVO> basicInfoMap = new HashMap<String, FarmDicVO>();
		String sql = "SELECT * FROM BASICINFO BI"
				+ " WHERE BI.farm_id IN("+sqlHelper.preparePlaceHolders(farmIds.size())+")";
		String farmIdsStr = Arrays.toString(farmIds.toArray()).replace("[", "").replace("]", "");
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql, farmIdsStr);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			String projectName = String.valueOf(row.get("province_name"));// 项目公司
			String fanNo = String.valueOf(row.get("fan_no"));// 风机编号
			String area = String.valueOf(row.get("region"));// 区域
			String farmName = String.valueOf(row.get("farm_name"));// 风场名称
			String farmShortName = String.valueOf(row.get("farm_nickname"));// 风场简称
			String farmID = String.valueOf(row.get("farm_id"));// 风场ID
			String fanName = String.valueOf(row.get("fan_name"));// 风机名称
			String fanType = String.valueOf(row.get("fan_type"));// 风机类型
			String fanCapacity = String.valueOf(row.get("capacity"));// 机组容量
			String fanFactory = String.valueOf(row.get("factory"));// 风机厂家
			String altitude = row.get("altitude") != null?String.valueOf(row.get("altitude")):null;// 海拔高度
			FarmDicVO farmDicVO = new FarmDicVO();
			farmDicVO.setProject(projectName);
			farmDicVO.setFanNo(fanNo);
			farmDicVO.setArea(area);
			farmDicVO.setFarmName(farmName);
			farmDicVO.setFarmShortName(farmShortName);
			farmDicVO.setFarmNo(farmID);
			farmDicVO.setFanName(fanName);
			farmDicVO.setFanType(fanType);
			farmDicVO.setFanCapacity(fanCapacity);
			farmDicVO.setFanFactory(fanFactory);
			farmDicVO.setAltitude(altitude);
			basicInfoMap.put(fanNo, farmDicVO);
		}
		return basicInfoMap;
	}
	
	/**
	 * 风机风场ID获取机组型号
	 * @param farmNos
	 * @return
	 * @throws SQLException 
	 */
	public Map<String, Set<String>> getFanTypeByFarmNO(List<String> farmIds) throws SQLException {
		Map<String, Set<String>> fanTypeMap = new HashMap<String, Set<String>>();
		String sql = "SELECT fan_type, farm_id FROM BASICINFO WHERE farm_id IN("+sqlHelper.preparePlaceHolders(farmIds.size())+")";
		String farmIdsStr = Arrays.toString(farmIds.toArray()).replace("[", "").replace("]", "");
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql, farmIdsStr);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			String fanType = String.valueOf(row.get("fan_type"));
			String farmID = String.valueOf(row.get("farm_id"));
			Set<String> fanTypeSet = fanTypeMap.get(farmID);
			if(fanTypeSet == null) {
				fanTypeSet = new HashSet<String>();
			}
			fanTypeSet.add(fanType);
			fanTypeMap.put(farmID, fanTypeSet);
		}
		return fanTypeMap;
	}

	/**
	 * 风机风场ID获取机组型号
	 * @param farmNos
	 * @return
	 * @throws SQLException 
	 */
	public List<String> getFarmShortNames(List<String> farmIds) throws SQLException {
		List<String> farmShortNames = new ArrayList<String>();
		String sql = "SELECT DISTINCT(farm_nickname) FROM BASICINFO WHERE farm_id IN("+sqlHelper.preparePlaceHolders(farmIds.size())+")";
		String farmIdsStr = Arrays.toString(farmIds.toArray()).replace("[", "").replace("]", "");
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql, farmIdsStr);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> row = resultList.get(i);
			String farmShortName = String.valueOf(row.get("farm_nickname"));
			farmShortNames.add(farmShortName);
		}
		return farmShortNames;
	}
	/**
	 * 查询风场下所有的机型信息
	 * 
	 * @param farmIds 多个风场ID
	 * @return key=机型，value=机型、容量、厂家信息。
	 * @throws SQLException 
	 */
	public Map<String, FarmDicVO> getFanTypeInfo(List<String> farmIds) throws SQLException{
		Map<String, FarmDicVO> fanTypeInfoMap = new HashMap<String, FarmDicVO>();
		String sql = "SELECT DISTINCT bi.fan_type,bi.capacity,bi.factory"
				+ " FROM BASICINFO bi WHERE bi.farm_id IN(" + sqlHelper.preparePlaceHolders(farmIds.size()) + ")";
		String farmIdsStr = Arrays.toString(farmIds.toArray()).replace("[", "").replace("]", "");
		List<Map<String, Object>> resultList = sqlHelper.executeQuery(sql, farmIdsStr);
		for(int i = 0; i < resultList.size(); i++) {
			Map<String, Object> rowMap = resultList.get(i);
			String fanType = String.valueOf(rowMap.get("fan_type"));
			String capacity = String.valueOf(rowMap.get("capacity"));
			String factory = String.valueOf(rowMap.get("factory"));
			FarmDicVO farmDicVO = new FarmDicVO();
			farmDicVO.setFanType(fanType);
			farmDicVO.setFanCapacity(capacity);
			farmDicVO.setFanFactory(factory);
			fanTypeInfoMap.put(fanType, farmDicVO);
		}
		return fanTypeInfoMap;
	}
	
	public void close(){
		sqlHelper.closeConnection();
	}
	
	public static void main(String[] args) throws SQLException {
		BasicInfoDao dao = new BasicInfoDao();
		List<String> farmIds = new ArrayList<String>();
		farmIds.add("8");
		farmIds.add("9");
		Map<String, FarmDicVO> farmDicMap = dao.getBasicInfoMap(farmIds);
		Set<String> keys = farmDicMap.keySet();
		for (String fanNo : keys) {
			FarmDicVO farmDicVO = farmDicMap.get(fanNo);
			System.out.println(farmDicVO);
		}
//		Map<String, Set<String>> fanTypeInfo = dao.getFanTypeByFarmNO(farmIds);
//		Set<String> keys = fanTypeInfo.keySet();
//		for(String farmID : keys) {
//			Set<String> fanTypes = fanTypeInfo.get(farmID);
//			System.out.println(Arrays.toString(fanTypes.toArray()));
//		}
	}
}
