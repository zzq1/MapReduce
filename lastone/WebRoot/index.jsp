<%@ page language="java" import="java.util.*" pageEncoding="utf-8"%>
<%
String path = request.getContextPath();
String basePath = request.getScheme()+"://"+request.getServerName()+":"+request.getServerPort()+path+"/";
%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    <base href="<%=basePath%>">
    
    <title>My JSP 'index.jsp' starting page</title>
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
    <center>
  <form action="dogetcon1.jsp"> 
  <table width="250"  height="100" border=1 cellspacing="0" bordercolor="black" bordercolorlight="black" bordercolordark="white" style="margin-top:200">
   <tr>
     <td align="center" heigth="40" bgcolor="lightgrey">连接状态：
        <%
          if(session.getAttribute("connectionstatus")==null){
        %>
               没有进行连接！
        <%
          }
          else{
        %>       
          <%=session.getAttribute("connectionstatus")%>     
        <%  
        	  session.invalidate();
          }
        %>
     </td>
   </tr>
   <tr>
     <td align="center" height="60" valign="middle">
        <input type="submit" value="获得连接">
     </td>
   </tr>
  </table>
  </form>
 </center>
  </body>
</html>