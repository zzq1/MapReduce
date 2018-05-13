package org.lxh.listenerdemo;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextAttributeEvent;
import javax.servlet.ServletContextAttributeListener;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRequestAttributeEvent;
import javax.servlet.ServletRequestAttributeListener;
import javax.servlet.ServletRequestEvent;
import javax.servlet.ServletRequestListener;
import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSessionActivationListener;
import javax.servlet.http.HttpSessionAttributeListener;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionIdListener;
import javax.servlet.http.HttpSessionListener;
import org.lxh.mvcdemo.vo.User;

/**
 * Application Lifecycle Listener implementation class OnlineUserList
 *
 */
@WebListener
public class OnlineUserList implements ServletContextListener, HttpSessionListener,  ServletRequestAttributeListener {

    /**
     * Default constructor. 
     */
	private ServletContext app =null;
	public void contextInitialized(ServletContextEvent arg0)  { 
    	this.app=arg0.getServletContext();
    	this.app.setAttribute("online",new TreeSet());
         // TODO Auto-generated method stub
}
    public void attributeAdded(ServletContextAttributeEvent event)  { 
         // TODO Auto-generated method stub
    	Set all =(Set)this.app.getAttribute("online");
    	all.add(event.getValue());
    	this.app.setAttribute("online",all);
    	
    }
  

	/**
     * @see ServletRequestAttributeListener#attributeRemoved(ServletRequestAttributeEvent)
     */
    public void attributeRemoved(ServletRequestAttributeEvent event)  { 
    	Set all =(Set)this.app.getAttribute("online");
    	all.remove(event.getValue());
    	this.app.setAttribute("online",all);
         // TODO Auto-generated method stub
    }
    public void sessionDestroyed(HttpSessionEvent event)  { 
    	User user =new  User();
    	Set all =(Set)this.app.getAttribute("online");
		all.remove( event.getSession().getAttribute("userid"));
		this.app.setAttribute("online", all);
         // TODO Auto-generated method stub
    }
	@Override
	public void attributeAdded(ServletRequestAttributeEvent srae) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void attributeReplaced(ServletRequestAttributeEvent srae) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void sessionCreated(HttpSessionEvent se) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		// TODO Auto-generated method stub
		
	}


	
}
