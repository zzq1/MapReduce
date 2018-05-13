package com.neu;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.neu.dao.BasicInfoDao;
import com.neu.dao.GPCDao;
import com.neu.vo.FarmDicVO;

/**
 * 初始化数据装�?
 * 
 * @author DLM
 *
 */
public class InitializeLoad {

	private List<String> farmList;// 多个风场ID

	/**
	 * 初始化数据参�?
	 */
	public static final String INIT_INITIAL_PARA = "InitialPara";
	
	/**
	 * 数据列索引参�?
	 */
	public static final String INIT_DATA_LINE_NO = "DataLineNo";
	
	/**
	 * 保证功率曲线参数
	 */
	public static final String INIT_GPC = "GPC";
	
	/**
	 * 月空气密度参�?
	 */
	public static final String INIT_ROU_MONTH = "RouMonth";
	
	/**
	 * 海拔初始化参�?
	 */
	public static final String INIT_ALTITUDE = "Altitude";
	
	/**
	 * NTF参数
	 */
	public static final String INIT_NTF_TABLE = "NTFTable";
	
	/**
	 * 转�?�功率参�?
	 */
	public static final String INIT_RS_TABLE = "RSTable";
	
	/**
	 * 台账数据
	 */
	public static final String INIT_DIC = "DIC";
	
	/**
	 * 过滤阈�??
	 */
	public static final String INIT_FILTER_PARA = "FilterPara";
	
	/**
	 * 风场_风机对应关系
	 */
	public static final String FARM_FAN = "farmAndFan";
	
	/**
	 * 风机类型信息集合
	 */
	public static final String FAN_TYPE_INFO_MAP = "fanTypeInfoMap";
	
	/**
	 * 十天功率曲线
	 */
	public static final String PCBIN_TENDAY_MAP = "pcbinTendayMap";

	public InitializeLoad(List<String> farmIds) {
		this.farmList = farmIds;
	}

	public InitializeLoad() {
	}

	/**
	 * 初始化参数的加载
	 * 
	 * @param startYearMonth 执行运算数据的开始年�?
	 * @param endYearMonth 执行运算数据的结束年�?
	 * 
	 * @return �?有初始化参数封装的Map对象�?
	 * @throws Exception
	 */
	public Map<String, Object> InitialzeParams() throws Exception{
		
		Map<String, Object> map = new HashMap<String, Object>();

		// 1. 加载台账信息
		Map<String, FarmDicVO> dicInfoMap = this.getDicInfoMap(farmList); 
		map.put(INIT_DIC, dicInfoMap);
		
		// 2. 加载机型信息，key=机型，value=机型、厂家�?�容�?
		Map<String, FarmDicVO> fanTypeInfoMap = this.getFanTypeInfo(farmList);
		
		// 3.获取风机类型（key=风场ID，value=机型列表�?
		Map<String, Set<String>> farmTypeInfo = this.getFanTypeByFarmNO(farmList);
		
		// 7. 获取GPC初始化参�?(每风场_机型�?条，key=风场_机型)
		Map<String, List<List<Double>>> gpcDF = this.getGPCList(farmList, farmTypeInfo, fanTypeInfoMap);
		map.put(INIT_GPC, gpcDF);
		
		return map;
	}
	
	/**
	 * 读取GPC初始化数�?
	 * @param farmIDs 多个风场ID
	 * @param farmTypeInfo 风场和机型的对应关系（key=风场ID，value=风场下多个机型列表）
	 * @param fanTypeInfoMap 机型信息。（key=机型，value=机型、厂家�?�容量）
	 * 
	 * @return GPC数据(每机型一�?)，key=机型
	 * @throws Exception 
	 */
	private Map<String, List<List<Double>>> getGPCList(List<String> farmIDs,
			Map<String, Set<String>> farmTypeInfo, Map<String, FarmDicVO> fanTypeInfoMap) throws Exception {
		GPCDao gpcDao = new GPCDao();
		List<String> fanTypeList = new ArrayList<String>();
		Set<String> keySet = farmTypeInfo.keySet();
		for (String key : keySet) {
			for (String type : farmTypeInfo.get(key)) {
				if (!fanTypeList.contains(type)) {
					fanTypeList.add(type);
				}
			}
		}
		// 获取GPC实例数据(每风场机型一�?)
		Map<String, List<List<Double>>> gpcMap = gpcDao.getGPC(farmIDs);
		// 获取GPC模板数据(每机型一�?)
		Map<String, List<List<Double>>> gpcTemplate = gpcDao.getGPCTemplate(fanTypeList);// GPC模板
		// 按照容量区分的所有GPC均�??
		Map<String, List<List<Double>>> gpcAvgCapacity = gpcDao.getGPCAvgByCapacity();
		Set<String> farmIds = farmTypeInfo.keySet();
		for (String farmID : farmIds) {
			Set<String> fanTypes = farmTypeInfo.get(farmID);
			for (String type : fanTypes) {
				String key = farmID + "_" + type;
				// 若GPC实例缺失，则默认使用GPC模板数据�?
				if(!gpcMap.containsKey(key)) {
					if(gpcTemplate.get(type) == null) {
						// 若GPC模板也不存在，则使用同容量其他GPC（平均�?�）�?
						String capacity = fanTypeInfoMap.get(type).getFanCapacity();
						gpcMap.put(key, gpcAvgCapacity.get(capacity));
					}else {
						gpcMap.put(key, gpcTemplate.get(type));
					}
				}
			}
		}
		gpcDao.close();
		return gpcMap;
	}

	/**
	 * 获取指定风场的台账信�?
	 * @param farmIds 多个风场ID
	 * @return 风场台账信息(key=风机编号,value=风机台账信息)
	 * @throws Exception 
	 */
	private Map<String, FarmDicVO> getDicInfoMap(List<String> farmIds) throws Exception {
		BasicInfoDao basicInfoDao = new BasicInfoDao();
		Map<String, FarmDicVO> farmDicMap = basicInfoDao.getBasicInfoMap(farmIds);
		basicInfoDao.close();
		return farmDicMap;
	}
	
	/**
	 * 加载风场下所有机型信息�??
	 * 
	 * @param farmIds 多个风场ID
	 * @return key=机型，value=机型、容量�?�厂家信息�??
	 * @throws Exception 
	 */
	private Map<String, FarmDicVO> getFanTypeInfo(List<String> farmIds) throws Exception{
		BasicInfoDao basicInfoDao = new BasicInfoDao();
		Map<String, FarmDicVO> fanTypeInfoMap = basicInfoDao.getFanTypeInfo(farmIds);
		basicInfoDao.close();
		return fanTypeInfoMap;
	}
	
	/**
	 * 根据风场 ID 获取风机编号
	 * 
	 * @param farmNos 多个风场ID
	 * @return 风机编号和风场ID对应的Map集合(key=风机编号,value=风场ID)
	 * @throws Exception 
	 */
	private Map<String, Set<String>> getFanTypeByFarmNO(List<String> farmIds) throws Exception {
		BasicInfoDao basicInfoDao = new BasicInfoDao();
		Map<String, Set<String>> farmTypeInfo =  basicInfoDao.getFanTypeByFarmNO(farmIds);
		basicInfoDao.close();
		return farmTypeInfo;
	}
}
