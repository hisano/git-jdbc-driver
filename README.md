# JDBC Driver for Git repository

This is an experimental project.

## Sample code

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Main {
	public static void main(String... args) throws Exception {
		Class.forName("jp.hisano.jdbc.git.Driver");
		try (Connection connection = DriverManager.getConnection("jdbc:git:GIT_REPOSITORY_DIRECTORY_PATH")) {
			try (Statement statement = connection.createStatement()) {
				try (ResultSet resultSet = statement.executeQuery("SELECT author, COUNT(*) AS commit_count FROM commits GROUP BY author")) {
					while (resultSet.next()) {
						System.out.println(resultSet.getString("author") + ": " + resultSet.getInt("commit_count"));
					}
				}
			}
		}
	}
}
```
