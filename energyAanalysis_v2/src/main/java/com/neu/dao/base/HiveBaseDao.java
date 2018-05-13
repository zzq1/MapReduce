package com.neu.dao.base;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.neu.Main;


public class HiveBaseDao {
	private final static Logger logger = Logger.getLogger(HiveBaseDao.class);

	private final static String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
	private static final String DB_URL = "jdbc:hive2://192.168.88.103:10000/default";
	private static final String USER = "hive";
	private static final String PASSWORD = "hive";
	private final static int FETCH_SIZE = 100000;
	private String dbUrl;
	private String user;
	private String pass;
	
	protected Properties prop = Main.prop;
	
//	static {
//		try {
//			Class.forName(DRIVER_CLASS);
//		} catch (ClassNotFoundException e) {
//			logger.error("Hive driver class not found", e);
//			System.exit(1);
//		}
//	}

	public HiveBaseDao() {
		if(null != this.prop){
			this.dbUrl = this.prop.getProperty("hive.jdbcUrl");
			this.user = this.prop.getProperty("hive.usrName");
			this.pass = this.prop.getProperty("hive.password");
		} else {
			this.dbUrl = DB_URL;
			this.user = USER;
			this.pass = PASSWORD;
		}
	}
	
	public Connection getConn() throws SQLException {
		try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection conn = DriverManager.getConnection(dbUrl, user, pass);
		return conn;
	}

	public List<Map<String, Object>> executeQuery(String sql, Object ...parms) throws SQLException {
		if (isBlank(sql)) {
			logger.info("Commands string is empty. skipping.");
			return null;
		}
		List<Map<String, Object>> lists = new ArrayList<Map<String, Object>>();
		PreparedStatement pstmt = null;
		Connection con = this.getConn();
		pstmt = con.prepareStatement(sql);
		pstmt.setFetchSize(FETCH_SIZE);
		int placeHolderIndex = 1;
		for(int i = 0; i < parms.length; i++) {
			if(parms[i] instanceof String) {
				String params = (String) parms[i];
				String[] paramArr = params.split(",");
				for(int j = 0; j < paramArr.length; j++) {
					pstmt.setObject(placeHolderIndex, paramArr[j].trim());
					placeHolderIndex++;
				}
			}else {
				pstmt.setObject(placeHolderIndex, parms[i]);
				placeHolderIndex++;
			}
		}
		ResultSet rs = pstmt.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        while (null != rs && rs.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int i = 0; i < columnCount; i++) {
                String name = rsmd.getColumnName(i + 1);
                Object value = rs.getObject(name);
                map.put(name.substring(name.indexOf('.') + 1), value);
            }
            lists.add(map);
        }
		pstmt.close();
		con.close();
		return lists;
	}
	
	/**
	 * 执行指定点SQL语句
	 * 
	 * @param sql sql语句
	 * @param parms 多个参数
	 * @throws SQLException 
	 */
	public int executeUpdate(String sql, Object ...parms) throws SQLException{
		PreparedStatement pstmt = null;
		Connection con = this.getConn();
		pstmt = con.prepareStatement(sql);
		int placeHolderIndex = 1;
		for(int i = 0; i < parms.length; i++) {
			if(parms[i] instanceof String) {
				String params = (String) parms[i];
				String[] paramArr = params.split(",");
				for(int j = 0; j < paramArr.length; j++) {
					pstmt.setObject(placeHolderIndex, paramArr[j].trim());
					placeHolderIndex++;
				}
			}else {
				pstmt.setObject(placeHolderIndex, parms[i]);
				placeHolderIndex++;
			}
		}
		int result = pstmt.executeUpdate();
		pstmt.close();
		con.close();
		return result;
	}
	
	/**
	 * 生成多个?。用于生成带In的sql语句。
	 * @param length
	 * @return
	 */
	public String preparePlaceHolders(int length) {

	    StringBuilder builder = new StringBuilder();

	    for (int i = 0; i < length;) {

	        builder.append("?");

	        if (++i < length) {

	            builder.append(",");

	        }

	    }
	    return builder.toString();
	}
}
