package org.lxh.mvcdemo.dao;
import org.lxh.mvcdemo.vo.User;

public interface IUserDAO {
	/*
	 * �û���¼��֤
	 * @param user ����vo����
	 * @return ��֤�Ĳ������
	 * @throw Exception
	 */
	public boolean findLogin(User user)throws Exception;

}
