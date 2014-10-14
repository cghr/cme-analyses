package org.cghr.cme25;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.cghr.cme25.DdxWrapper;

/**
 * Hello world!
 *
 */
public class DeceptionDetect {
	private String context = "tp";
	private String jdbcDriver = "com.mysql.jdbc.Driver";
	private String jdbcUrl = "jdbc:mysql://localhost:3306/database-name";
	private String jdbcUsername = "su";
	private String jdbcPassword = "su";
	private String icdTable = "icd_codes";
	private String icdEquivalenceTable = "icd_equivalent_codes";

	private String icdEquivalenceColumn = "icdEquivalence";

	private String p1CodingIcdColumn = "";
	private String p2CodingIcdColumn = "";
	private String p1ReconciliationIcdColumn = "";
	private String p2ReconciliationIcdColumn = "";
	private String adjudicationIcdColumn = "";
	private String p1CodingDdxColumn = "";
	private String p2CodingDdxColumn = "";
	private String p1ReconciliationDdxColumn = "";
	private String p2ReconciliationDdxColumn = "";
	private String p1CodingSimilarColumn = "";
	private String p2CodingSimilarColumn = "";
	private String p1ReconciliationSimilarColumn = "";
	private String p2ReconciliationSimilarColumn = "";

	private Connection connection = null;

	private DdxWrapper ddxWrapper = DdxWrapper.getInstance();

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public void setJdbcUsername(String jdbcUsername) {
		this.jdbcUsername = jdbcUsername;
	}

	public void setJdbcPassword(String jdbcPassword) {
		this.jdbcPassword = jdbcPassword;
	}

	public void setIcdTable(String icdTable) {
		this.icdTable = icdTable;
	}

	public void setIcdEquivalenceTable(String icdEquivalenceTable) {
		this.icdEquivalenceTable = icdEquivalenceTable;
	}

	public void setIcdEquivalenceColumn(String icdEquivalenceColumn) {
		this.icdEquivalenceColumn = icdEquivalenceColumn;
	}

	public void setP1CodingIcdColumn(String p1CodingIcdColumn) {
		this.p1CodingIcdColumn = p1CodingIcdColumn;
	}

	public void setP2CodingIcdColumn(String p2CodingIcdColumn) {
		this.p2CodingIcdColumn = p2CodingIcdColumn;
	}

	public void setP1ReconciliationIcdColumn(String p1ReconciliationIcdColumn) {
		this.p1ReconciliationIcdColumn = p1ReconciliationIcdColumn;
	}

	public void setP2ReconciliationIcdColumn(String p2ReconciliationIcdColumn) {
		this.p2ReconciliationIcdColumn = p2ReconciliationIcdColumn;
	}

	public void setAdjudicationIcdColumn(String adjudicationIcdColumn) {
		this.adjudicationIcdColumn = adjudicationIcdColumn;
	}

	public void setP1CodingDdxColumn(String p1CodingDdxColumn) {
		this.p1CodingDdxColumn = p1CodingDdxColumn;
	}

	public void setP2CodingDdxColumn(String p2CodingDdxColumn) {
		this.p2CodingDdxColumn = p2CodingDdxColumn;
	}

	public void setP1ReconciliationDdxColumn(String p1ReconciliationDdxColumn) {
		this.p1ReconciliationDdxColumn = p1ReconciliationDdxColumn;
	}

	public void setP2ReconciliationDdxColumn(String p2ReconciliationDdxColumn) {
		this.p2ReconciliationDdxColumn = p2ReconciliationDdxColumn;
	}

	public void setP1CodingSimilarColumn(String p1CodingSimilarColumn) {
		this.p1CodingSimilarColumn = p1CodingSimilarColumn;
	}

	public void setP2CodingSimilarColumn(String p2CodingSimilarColumn) {
		this.p2CodingSimilarColumn = p2CodingSimilarColumn;
	}

	public void setP1ReconciliationSimilarColumn(String p1ReconciliationSimilarColumn) {
		this.p1ReconciliationSimilarColumn = p1ReconciliationSimilarColumn;
	}

	public void setP2ReconciliationSimilarColumn(String p2ReconciliationSimilarColumn) {
		this.p2ReconciliationSimilarColumn = p2ReconciliationSimilarColumn;
	}

	private DeceptionDetect(String context) {
		this.context = context;
	}

	public static DeceptionDetect getInstance(String context) {
		return new DeceptionDetect(context);
	}

	private Connection getConnection() {
		try {
			if (connection == null || connection.isClosed()) {
				connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
			}

			return connection;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void updateIsDdx(String icd1Column, String icd2Column, String updateColumn) {
		try {
			Connection connection = getConnection();

			String getIcdColumnsQuery = "SELECT " + icd1Column + "," + icd2Column + " FROM " + icdTable;

			Statement getIcdColumnsStatement = connection.createStatement();
			ResultSet icdColumnsResultSet = getIcdColumnsStatement.executeQuery(getIcdColumnsQuery);

			connection.setAutoCommit(false);

			Statement updateStatement = connection.createStatement();

			while (icdColumnsResultSet.next()) {
				String icd1 = icdColumnsResultSet.getString(icd1Column).trim();
				String icd2 = icdColumnsResultSet.getString(icd2Column).trim();

				Boolean isDdx = ddxWrapper.isDdxOf(icd1, icd2);

				String isDdxValue = isDdx ? "T" : "F";

				String updateQuery = "UPDATE " + icdTable + " SET " + updateColumn + "='" + isDdxValue + "' WHERE " + icd1Column + "='" + icd1 + "' AND " + icd2Column + "='" + icd2 + "'";

				updateStatement.addBatch(updateQuery);
			}

			updateStatement.executeBatch();

			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void updateIsSimilar(String icd1Column, String icd2Column, String updateColumn) {
		try {
			Connection connection = getConnection();

			String getIcdColumnsQuery = "SELECT " + icd1Column + "," + icd2Column + " FROM " + icdTable;

			Statement getIcdColumnsStatement = connection.createStatement();
			ResultSet icdColumnsResultSet = getIcdColumnsStatement.executeQuery(getIcdColumnsQuery);

			connection.setAutoCommit(false);

			Statement updateStatement = connection.createStatement();

			while (icdColumnsResultSet.next()) {
				String icd1 = icdColumnsResultSet.getString(icd1Column).trim();
				String icd2 = icdColumnsResultSet.getString(icd2Column).trim();

				Boolean isSimilar = isSimilarTo(icd1, icd2);

				String isSimilarValue = isSimilar ? "T" : "F";

				String updateQuery = "UPDATE " + icdTable + " SET " + updateColumn + "='" + isSimilarValue + "' WHERE " + icd1Column + "='" + icd1 + "' AND " + icd2Column + "='" + icd2 + "'";

				updateStatement.addBatch(updateQuery);
			}

			updateStatement.executeBatch();

			connection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isSimilarTo(String icd1, String icd2) {
		try {
			Connection connection = getConnection();
			
			String isSimilarQuery = "SELECT IF(COUNT(*) > 0, 'T', 'F') isSimilar FROM " + icdEquivalenceTable + " WHERE " + icdEquivalenceColumn + " LIKE '%" + icd1 + "%' and " + icdEquivalenceColumn + " LIKE '%" + icd2 + "%'";
			
			Statement isSimilarStatement = connection.createStatement();
			ResultSet rs = isSimilarStatement.executeQuery(isSimilarQuery);
			
			rs.next();
			
			String isSimilarString = rs.getString("isSimilar");
			
			if(isSimilarString.equals("T")) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
	}

	public void updateIsP1CodingDdx() {
		updateIsDdx(p1CodingIcdColumn, adjudicationIcdColumn, p1CodingDdxColumn);
	}

	public void updateIsP2CodingDdx() {
		updateIsDdx(p2CodingIcdColumn, adjudicationIcdColumn, p2CodingDdxColumn);
	}

	public void updateIsP1ReconciliationDdx() {
		updateIsDdx(p1ReconciliationIcdColumn, adjudicationIcdColumn, p1ReconciliationDdxColumn);
	}

	public void updateIsP2ReconciliationDdx() {
		updateIsDdx(p2ReconciliationIcdColumn, adjudicationIcdColumn, p2ReconciliationDdxColumn);
	}
	
	public void updateIsP1CodingSimilar() {
		updateIsSimilar(p1CodingIcdColumn, adjudicationIcdColumn, p1CodingSimilarColumn);
	}
	
	public void updateIsP2CodingSimilar() {
		updateIsSimilar(p2CodingIcdColumn, adjudicationIcdColumn, p2CodingSimilarColumn);
	}
	
	public void updateIsP1ReconciliationSimilar() {
		updateIsSimilar(p1ReconciliationIcdColumn, adjudicationIcdColumn, p1ReconciliationSimilarColumn);
	}
	
	public void updateIsP2ReconciliationSimilar() {
		updateIsSimilar(p2ReconciliationIcdColumn, adjudicationIcdColumn, p2ReconciliationSimilarColumn);
	}

	public static void main(String[] args) {
		DeceptionDetect deceptionDetect = DeceptionDetect.getInstance("sn");
		deceptionDetect.setJdbcDriver("com.mysql.jdbc.Driver");
		deceptionDetect.setJdbcUrl("jdbc:mysql://localhost:3306/cme_deception");
		deceptionDetect.setJdbcUsername("senthil");
		deceptionDetect.setJdbcPassword("c8pfu7j");
		deceptionDetect.setIcdTable("icd_codes");

		deceptionDetect.setIcdTable("icd_codes");
		deceptionDetect.setP1CodingIcdColumn("p1_coding");
		deceptionDetect.setP2CodingIcdColumn("p2_coding");
		deceptionDetect.setP1ReconciliationIcdColumn("p1_reconciliation");
		deceptionDetect.setP2ReconciliationIcdColumn("p2_reconciliation");
		deceptionDetect.setAdjudicationIcdColumn("p3_adjudication");
		deceptionDetect.setP1CodingDdxColumn("p1_c_ddx");
		deceptionDetect.setP2CodingDdxColumn("p2_c_ddx");
		deceptionDetect.setP1ReconciliationDdxColumn("p1_r_ddx");
		deceptionDetect.setP2ReconciliationDdxColumn("p2_r_ddx");

		deceptionDetect.setIcdEquivalenceTable("icd_equivalent_codes");
		deceptionDetect.setIcdEquivalenceColumn("icdEquivalence");

		deceptionDetect.updateIsP1CodingDdx();
		deceptionDetect.updateIsP2CodingDdx();
		deceptionDetect.updateIsP1ReconciliationDdx();
		deceptionDetect.updateIsP2ReconciliationDdx();
		
		deceptionDetect.setP1CodingSimilarColumn("p1_c_similar");
		deceptionDetect.setP2CodingSimilarColumn("p2_c_similar");
		deceptionDetect.setP1ReconciliationSimilarColumn("p1_r_similar");
		deceptionDetect.setP2ReconciliationSimilarColumn("p2_r_similar");

		deceptionDetect.updateIsP1CodingSimilar();
		deceptionDetect.updateIsP2CodingSimilar();
		deceptionDetect.updateIsP1ReconciliationSimilar();
		deceptionDetect.updateIsP2ReconciliationSimilar();
	}
}
