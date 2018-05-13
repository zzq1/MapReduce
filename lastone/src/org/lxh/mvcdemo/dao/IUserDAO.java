package org.lxh.mvcdemo.dao;
import org.lxh.mvcdemo.vo.User;

public interface IUserDAO {
	/*
	 * 用户登录验证
	 * @param user 传入vo对象
	 * @return 验证的操作结果
	 * @throw Exception
	 */
	public boolean findLogin(User user)throws Exception;

}
