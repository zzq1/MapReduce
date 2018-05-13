package com.neu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBUtil {

	// 连接数据库的参数
	private static String url = null;

	private static String user = null;

	private static String driver = null;

	private static String password = null;

	private static DBUtil instance = null;

	public static DBUtil getInstance() {
		if (instance == null) {
			synchronized (DBUtil.class) {
				if (instance == null) {
					instance = new DBUtil();
				}
			}
		}
		return instance;
	}

	// 配置文件
	private static Properties prop = new Properties();

	// 注册驱动
	static {
		try {
			// 利用类加载器读取配置文件
			InputStream is = DBUtil.class.getClassLoader()
					.getResourceAsStream("dbInfo.properties");
			prop.load(is);
			url = prop.getProperty("url");
			user = prop.getProperty("user");
			driver = prop.getProperty("driver");
			password = prop.getProperty("password");

			Class.forName(driver);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// 该方法获得连接
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}

	// 释放资源
	public void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {

				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) throws SQLException {
		DBUtil dbUtil = DBUtil.getInstance();
		Connection conn = dbUtil.getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM ALTITUDE");
		while(rs.next()) {
			System.out.println(rs.getString(1) + "," + rs.getString(2));
		}
	}
}
