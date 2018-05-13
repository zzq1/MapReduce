/**
 * 
 */
/**
 * @author Administrator
 *
 */
package org.lxh.mvcdemo.dao.proxy;
import org.lxh.mvcdemo.dao.IUserDAO;
import org.lxh.mvcdemo.dao.impl.UserDAOImpl;
import org.lxh.mvcdemo.dbc.DatabaseConnection;
import org.lxh.mvcdemo.vo.User;
public class UserDAOProxy implements IUserDAO{
	private DatabaseConnection dbc= null;
	private IUserDAO dao = null;
	public UserDAOProxy(){
	try{
		this.dbc=new DatabaseConnection();
	}catch(Exception e ){
		e.printStackTrace();
	}
	this.dao=new UserDAOImpl( this.dbc.getCon());
}
	@Override
	public boolean findLogin(User user) throws Exception {
		// TODO Auto-generated method stub
		boolean flag=false;
		try{
			flag=this.dao.findLogin(user);
		}catch(Exception e){
			throw e;
		}finally{
			this.dbc.closed();
		}
		return flag;
	}
}