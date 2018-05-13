package org.lxh.mvcdemo.servlet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.mail.Session;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import org.lxh.mvcdemo.factory.DAOFactory;
import org.lxh.mvcdemo.vo.User;
public class LoginServlet extends HttpServlet {
	
	public  void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	
		String path ="login.jsp";
		String userid = request.getParameter("userid");
		String userpass =request.getParameter("userpass");
		List<String> info=new ArrayList<String>();
		if(userid==null||"".equals(userid)){
			info.add("用户id不存在!");
			
		}
		if(userpass==null||"".equals(userpass)){
			info.add("密码不能为空!");
		}
		if(info.size()==0){
			User user= new User();
			user.setUseid(userid);
			user.setPassword(userpass);
			try{
				if(DAOFactory.getIUserDAOInstance().findLogin(user)){
				
				HttpSession session  = request.getSession();
				session.setAttribute("userid", userid);
				request.getRequestDispatcher("suc.jsp").forward(request, response);
				}else{
					info.add("用户登录失败！");
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
			request.setAttribute("info",info);
			request.getRequestDispatcher(path).forward(request, response);
		
	}


	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		 this.doGet(request, response);
	}

}
