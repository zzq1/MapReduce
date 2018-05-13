package org.lxh.mvcdemo.factory;
import org.lxh.mvcdemo.dao.IUserDAO;
import org.lxh.mvcdemo.dao.proxy.UserDAOProxy;
public class DAOFactory {
	public static IUserDAO getIUserDAOInstance(){
		return new UserDAOProxy();
	}

}
