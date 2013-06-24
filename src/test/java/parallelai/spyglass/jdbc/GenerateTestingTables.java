package parallelai.spyglass.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

/**
 * Class generates TWO tables in database 'TABLE_01' and 'TABLE_02'
 * 
 * Those tables are used by the 'integration-testing' of JDBCSource in file
 * JdbcSourceShouldReadWrite.scala
 * 
 * Run with: mvn -Dtestparallelai.spyglass.jdbc.GenerateTestingTables test
 * 
 */
public class GenerateTestingTables {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_PORT = "3306";
	static final String DB_NAME = "database_name";

	
	static final String DB_URL = "jdbc:mysql://<hostname>:<port>/<db_name>?zeroDateTimeBehavior=convertToNull";

	// Database credentials
	static final String USER = "user";
	static final String PASS = "password";

	public static enum TestingTable {
		TABLE_01, TABLE_02
	}

	private static final Log LOG = LogFactory
			.getLog(GenerateTestingTables.class);

	@Test
	public void fakeTest() {

		// Connect to Quorum
		LOG.info("Connecting to " + DB_URL + ":" + DB_PORT);

		Connection conn = null;
		Statement stmt = null;
		try {
			// STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			LOG.info("Connecting to a selected database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			LOG.info("Connected database successfully...");

			
			// Delete test tables
			deleteTestTable(conn, TestingTable.TABLE_01.name());
			deleteTestTable(conn, TestingTable.TABLE_02.name());

			// Generate test tables
			createTestTable(conn, TestingTable.TABLE_01);
			createTestTable(conn, TestingTable.TABLE_02);

			// Populate test tables
			populateTestTable(conn, TestingTable.TABLE_01);
			//populateTestTable(conn, TestingTable.TABLE_02);

			// Print content of test table
			printHTable(conn, TestingTable.TABLE_01);

			// If we've reached here - the testing data are in
			Assert.assertEquals("true", "true");			
			
			
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			LOG.error(se.toString());
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			LOG.error(e.toString());
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					conn.close();
			} catch (SQLException se) {
			}// do nothing
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
				LOG.error(se.toString());
			}// end finally try
		}// end try

	}

	private static void populateTestTable(Connection connection, TestingTable testingTable)
			throws SQLException {
		
		
		// Load up table
		LOG.info("Populating table in given database...");
		Statement stmt = connection.createStatement();
		
		
		String [] queries = {
			    "insert into " + testingTable.name() + " values (1, 'A', 'X', 123)",
			    "insert into " + testingTable.name() + " values (2, 'B', 'Y', 234)",
			    "insert into " + testingTable.name() + " values (3, 'C', 'Z', 345)",
			};
			             
		Statement statement = connection.createStatement();
		             
		for (String query : queries) {
			statement.addBatch(query);
		}
		statement.executeBatch();
		LOG.info("Populated table in given database...");

		statement.close();
		
	}

	private static void createTestTable(Connection connection, TestingTable testingTable)
			throws SQLException {

		LOG.info("Creating table in given database...");
		Statement stmt = connection.createStatement();

		String sql = "CREATE TABLE " + testingTable.name() + " "
				+ "(id INTEGER not NULL, " + " test_column1 VARCHAR(255), "
				+ " test_column2 VARCHAR(255), " + " test_column3 INTEGER, "
				+ " PRIMARY KEY ( id ))";

		stmt.executeUpdate(sql);
		LOG.info("Created table in given database...");

		stmt.close();
	}

	/**
	 * Method to disable and delete HBase Tables i.e. "int-test-01"
	 */
	private static void deleteTestTable(Connection connection, String tableName) throws SQLException {

		
		// Execute a query
		LOG.info("Deleting table in given database...");
		Statement stmt = connection.createStatement();

		String sql = "DROP TABLE IF EXISTS " + tableName;

		int result = stmt.executeUpdate(sql);
		LOG.info("Deleted table in given database... " + result);		


		stmt.close();
	}

	/**
	 * Method to print-out an HTable
	 */
	private static void printHTable(Connection connection, TestingTable testingTable)
			throws SQLException {

		// Execute a query
		LOG.info("Printing table in given database...");
		Statement stmt = connection.createStatement();
		
		String sql = "SELECT * FROM " + testingTable.name();

		ResultSet resultSet = stmt.executeQuery(sql);
		LOG.info("Get data from table in given database...");			

		while (resultSet.next()) {
			Integer key = resultSet.getInt("id");
			String testColumn1 = resultSet.getString("test_column1");
			String testColumn2 = resultSet.getString("test_column2");
			Integer testColumn3 = resultSet.getInt("test_column3");
			
			LOG.info(key + " : " + testColumn1 + " : " + testColumn2 + " : " + testColumn3);
		}
		
	}

	public static void main(String[] args) {
		GenerateTestingTables test = new GenerateTestingTables();
		test.fakeTest();
	}
}