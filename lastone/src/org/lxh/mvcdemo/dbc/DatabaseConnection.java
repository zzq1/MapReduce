/**
 * 
 */
/**
 * @author Administrator
 *
 */
package org.lxh.mvcdemo.dbc;
import java.sql.*;

public class DatabaseConnection {
    private Connection con;
    private String classname="com.mysql.jdbc.Driver";
    private String url="jdbc:mysql://172.17.11.156:3306/mldn";
    private String user = "hive";
    private String password = "123456";
    public DatabaseConnection(){
    	try{
    		Class.forName(classname);
    	}
    	catch(ClassNotFoundException e){
    		e.printStackTrace();
    	}
    }
    public Connection getCon(){    	  
    	  try{
    	      con=DriverManager.getConnection(url,user,password);
    	  }
    	  catch(Exception e){
    		  e.printStackTrace(System.err);
    		  con=null;
    	  }
    	  return con;
    }
    public void closed(){
    	try{
    		if(con!=null)con.close();
    	}
    	catch(Exception e){e.printStackTrace();}    	
    }
}
