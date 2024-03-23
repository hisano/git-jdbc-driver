package jp.hisano.jdbc.git;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;
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
import org.h2.table.FunctionTable;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Utils;
import org.h2.value.ValueVarchar;

import static java.util.Arrays.*;

public class Driver implements java.sql.Driver {
	private static final Driver INSTANCE = new Driver();

	private static final AtomicBoolean IS_REGISTERED = new AtomicBoolean();

	private static final String ACCEPTABLE_URL_PREFIX = "jdbc:git:";

	private static final int VERSION_MAJOR = 1;
	private static final int VERSION_MINOR = 0;

	private static final String TABLE_COMMITS = "COMMITS";

	static {
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}

		registerDriver();
	}

	private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
		throw (E) e;
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
			addFunctionTable(gitRepositoryDirectoryPath, sessionLocal, TABLE_COMMITS);

			return jdbcConnection;
		} else {
			return null;
		}
	}

	private void addFunctionTable(String gitRepositoryDirectoryPath, SessionLocal sessionLocal, String tableName) {
		Schema mainSchema = sessionLocal.getDatabase().getMainSchema();

		FunctionAlias functionAlias = FunctionAlias.newInstance(mainSchema, 0, tableName, Driver.class.getName() + ".git", false);
		List<Expression> arguments = Utils.newSmallArrayList();
		arguments.add(ValueExpression.get(ValueVarchar.get(gitRepositoryDirectoryPath, sessionLocal)));
		arguments.add(ValueExpression.get(ValueVarchar.get(tableName, sessionLocal)));
		JavaTableFunction javaTableFunction = new JavaTableFunction(functionAlias, arguments.toArray(new Expression[0]));

		sessionLocal.getDatabase().addSchemaObject(sessionLocal, new FunctionTable(mainSchema, sessionLocal, javaTableFunction));
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

	public static ResultSet git(Connection connection, String gitRepositoryDirectoryPath, String tableName) throws Exception {
		Column[] columns = null;
		BiConsumer<Git, SimpleResultSet> rowAdder = null;
		switch (tableName) {
			case TABLE_COMMITS:
				columns = new Column[]{
						new Column("AUTHOR", Types.VARCHAR),
						new Column("MESSAGE", Types.VARCHAR)
				};
				rowAdder = (git, simpleResultSet) -> {
					try {
						git.log().call().forEach(commit -> {
							simpleResultSet.addRow(
									commit.getAuthorIdent().getName(),
									commit.getFullMessage()
							);
						});
					} catch (GitAPIException e) {
						sneakyThrow(e);
					}
				};
				break;
		}

		SimpleResultSet simpleResultSet = new SimpleResultSet();

		asList(columns).forEach(column -> simpleResultSet.addColumn(column._name, column._sqlType, 0, 0));

		String url = connection.getMetaData().getURL();
		if (url.equals("jdbc:columnlist:connection")) {
			return simpleResultSet;
		}

		try (Repository repository = new FileRepositoryBuilder().setGitDir(new File(gitRepositoryDirectoryPath, ".git")).readEnvironment().findGitDir().build()) {
			try (Git git = new Git(repository)) {
				rowAdder.accept(git, simpleResultSet);
			}
		}

		return simpleResultSet;
	}

	private static class Column {
		private String _name;
		private int _sqlType;

		Column(String name, int sqlType) {
			_name = name;
			_sqlType = sqlType;
		}
	}
}
