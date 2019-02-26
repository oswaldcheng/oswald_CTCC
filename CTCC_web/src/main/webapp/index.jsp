<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page language="java" contentType="text/html; charset=utf-8"
         pageEncoding="utf-8" %>
<%
    String path = request.getContextPath();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Show Time</title>
</head>
<body>
<form action="/queryCallLogList" method="post">
    手机号码:<input type="text", name="telephone">
    年:<input type="text", name="year">
    月:<input type="text", name="month">
    日:<input type="text", name="day">
    <input type="submit" value="查询">
</form>
时间原因，仅仅做好按全年月份查询每月通话次数（折线图，饼图），通信次数最大最小月份及全年每月通讯平均次数。
查询请先默认月和日为-1。
过阵子补上。
</body>
</html>