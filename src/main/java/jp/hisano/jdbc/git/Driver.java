package jp.hisano.jdbc.git;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.expression.function.table.JavaTableFunction;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.schema.FunctionAlias;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.table.FunctionTable;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Utils;
import org.h2.value.ValueVarchar;

public class Driver implements java.sql.Driver {
	private static final Driver INSTANCE = new Driver();

	private static final AtomicBoolean IS_REGISTERED = new AtomicBoolean();

	private static final String ACCEPTABLE_URL_PREFIX = "jdbc:git:";

	private static final int VERSION_MAJOR = 1;
	private static final int VERSION_MINOR = 0;

	static {
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}

		registerDriver();
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (url == null) {
			throw DbException.getJdbcSQLException(ErrorCode.URL_FORMAT_ERROR_2, null, Constants.URL_FORMAT, null);
		}

		if (acceptsURL(url)) {
			String gitRepositoryDirectoryPath = url.substring(ACCEPTABLE_URL_PREFIX.length());

			JdbcConnection jdbcConnection = new JdbcConnection("jdbc:h2:mem:", info, null, null, false);

			SessionLocal sessionLocal = (SessionLocal) jdbcConnection.getSession();
			sessionLocal.getDatabase().addSchemaObject(sessionLocal, getCommitsTable(sessionLocal, gitRepositoryDirectoryPath));

			return jdbcConnection;
		} else {
			return null;
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (url == null) {
			throw DbException.getJdbcSQLException(ErrorCode.URL_FORMAT_ERROR_2, null, Constants.URL_FORMAT, null);
		}
		return url.startsWith(ACCEPTABLE_URL_PREFIX);
	}

	@Override
	public int getMajorVersion() {
		return VERSION_MAJOR;
	}

	@Override
	public int getMinorVersion() {
		return VERSION_MINOR;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
		return new DriverPropertyInfo[0];
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	@Override
	public Logger getParentLogger() {
		return null;
	}

	private static synchronized void registerDriver() {
		try {
			if (IS_REGISTERED.compareAndSet(false, true)) {
				DriverManager.registerDriver(INSTANCE);
			}
		} catch (SQLException e) {
			DbException.traceThrowable(e);
		}
	}

	private static SchemaObject getCommitsTable(SessionLocal session, String gitRepositoryDirectoryPath) {
		Schema mainSchema = session.getDatabase().getMainSchema();

		FunctionAlias functionAlias = FunctionAlias.newInstance(mainSchema, 0, "COMMITS", Driver.class.getName() + ".git", false);
		ArrayList<Expression> argList = Utils.newSmallArrayList();
		argList.add(ValueExpression.get(ValueVarchar.get(gitRepositoryDirectoryPath, session)));
		argList.add(ValueExpression.get(ValueVarchar.get("COMMITS", session)));
		JavaTableFunction javaTableFunction = new JavaTableFunction(functionAlias, argList.toArray(new Expression[0]));

		return new FunctionTable(mainSchema, session, javaTableFunction);
	}

	public static ResultSet git(Connection connection, String gitRepositoryPath, String tableName) throws Exception {
		SimpleResultSet result = new SimpleResultSet();
		result.addColumn("AUTHOR", Types.VARCHAR, 0, 0);
		result.addColumn("MESSAGE", Types.VARCHAR, 0, 0);

		String url = connection.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")) {
			return result;
		}

		FileRepositoryBuilder repositoryBuilder = new FileRepositoryBuilder();
		try (Repository repository = repositoryBuilder.setGitDir(new File(gitRepositoryPath, ".git")).readEnvironment().findGitDir().build()) {
			try (Git git = new Git(repository)) {
				Iterable<RevCommit> commits = git.log().call();
				for (RevCommit commit : commits) {
					result.addRow(
							commit.getAuthorIdent().getName(),
							commit.getFullMessage()
					);
				}
			}
		}
		return result;
	}
}
