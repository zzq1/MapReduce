<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    <base href="<%=basePath%>">
    
    <title>My JSP 'login.jsp' starting page</title>
    
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="cache-control" content="no-cache">
	<meta http-equiv="expires" content="0">    
	<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
	<meta http-equiv="description" content="This is my page">
	<!--
	<link rel="stylesheet" type="text/css" href="styles.css">
	-->

  </head>
  
  <body>
  
  <%
  		List<String> info = (List<String>) request.getAttribute("info");
  		if(info != null){
  		Iterator<String> iter = info.iterator();
  		while(iter.hasNext()){
  		%>
  		<h4><%=iter.next() %></h4>
  		<%}
  		} %>
  		<form action="LoginServlet" method ="post" >
  		  用户ID:<input type="text" name="userid"><br>
  		  <tr>
  		  <td></td><br>
  		  </tr>
  		  密&nbsp;&nbsp;码:<input type="password" name="userpass"><br>
  		  <input type="submit" value="登录">
  		  <input type="reset" value="重置"> 
  		  </form>
  </body>
</html>
