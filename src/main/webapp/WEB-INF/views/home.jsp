<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ page session="false"%>
<html>
<head>
<title>Home</title>
</head>
<body>
	<h1>Spring data Redis Example</h1>

	<br>
	<form action="addUser" method="post">
		<input type="text" name="key" placeholder="key"> <br> <input
			type="text" name="value" placeholder="value"> <br> <input
			type="text" name="email" placeholder="email"> <br>
		<button type="submit">Save</button>
	</form>

	<br>
	<form action="getUser" method="get">
		<input type="text" name="key" placeholder="key"> <br>
		<button type="submit">Get</button>
		<br>
		<c:out value="${USER.name}" />
		<c:out value="${USER.email}" />
	</form>
	<br>
	<a href="getAllUser">Get All User</a> &nbsp;
	<a href="deleteAllUser">Delete All Users</a>
	<ul>
		<h3>User-ID | User Name</h3>
		<c:forEach items="${ userList }" var="user">
			<li><i><c:out value="${user.id}" /></i> | <i><c:out
						value="${user.name}" /></i> | <i><c:out value="${user.email}" /></i></li>
		</c:forEach>
	</ul>
</body>
</html>
