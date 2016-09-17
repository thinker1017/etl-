package com.tcl.conf;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LTConfigCtl extends ETLConfigCtl {

	public LTConfigCtl(String propertiesFilePath) throws IOException {
		super(propertiesFilePath);
	}

	@Override
	public List<ArrayList<String>> getBaseETLConfs()
			throws ClassNotFoundException, SQLException {
		List<ArrayList<String>> allConfs = new ArrayList<ArrayList<String>>();
		Connection conn = utils.getMySQLConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM engine_baseetl");
		System.out.println("------------------------get baseetls!");
		while (rs.next()) {
			ArrayList<String> TEMPLATE_CONF = new ArrayList<String>();
			TEMPLATE_CONF.add(0, rs.getString("template"));
			TEMPLATE_CONF.add(1, rs.getString("title"));
			allConfs.add(TEMPLATE_CONF);
		}
		stmt.close();
		conn.close();
		return allConfs;
	}

	@Override
	public List<ArrayList<String>> getCumulativesETLConfs()
			throws ClassNotFoundException, SQLException {
		List<ArrayList<String>> allConfs = new ArrayList<ArrayList<String>>();
		Connection conn = utils.getMySQLConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM engine_cumulativesetl");
		System.out.println("------------------------get cumuetls!");
		while (rs.next()) {
			ArrayList<String> TEMPLATE_INSERT_CONF = new ArrayList<String>();
			TEMPLATE_INSERT_CONF.add(0, rs.getString("template"));
			TEMPLATE_INSERT_CONF.add(1, rs.getString("title"));
			allConfs.add(TEMPLATE_INSERT_CONF);
		}
		stmt.close();
		conn.close();
		return allConfs;
	}

	@Override
	public List<ArrayList<String>> getConceptionETLConfs()
			throws ClassNotFoundException, SQLException {
		List<ArrayList<String>> allConfs = new ArrayList<ArrayList<String>>();
		Connection conn = utils.getMySQLConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("SELECT * FROM engine_customconception");
		System.out.println("------------------------get custometls!");
		while (rs.next()) {
			ArrayList<String> TEMPLATE_INSERT_CONF = new ArrayList<String>();
			TEMPLATE_INSERT_CONF.add(0, rs.getString("template"));
			TEMPLATE_INSERT_CONF.add(1, rs.getString("title"));
			allConfs.add(TEMPLATE_INSERT_CONF);
		}
		stmt.close();
		conn.close();
		return allConfs;
	}

	@Override
	public List<ArrayList<String>> getReportETLConfs(String reports)
			throws ClassNotFoundException, SQLException {
		List<ArrayList<String>> allConfs = new ArrayList<ArrayList<String>>();
		Connection conn = utils.getMySQLConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM engine_reportetl");
		System.out.println("------------------------get reportetls!");
		Map<String, Integer> validReportsMap = new HashMap<String, Integer>();
		if (reports != null) {
			String[] validReports = reports.split(",");
			for (String validReport : validReports) {
				validReportsMap.put(validReport, 0);
			}
		}

		while (rs.next()) {
			ArrayList<String> TEMPLATE_INSERT_CONF = new ArrayList<String>();
			TEMPLATE_INSERT_CONF.add(0, rs.getString("template"));
			TEMPLATE_INSERT_CONF.add(1, rs.getString("insert_conf"));
			TEMPLATE_INSERT_CONF.add(2, rs.getString("title"));
			String title = rs.getString("title");
			if (validReportsMap.size() == 0) {
				allConfs.add(TEMPLATE_INSERT_CONF);
			} else {
				if (validReportsMap.containsKey(title)) {
					allConfs.add(TEMPLATE_INSERT_CONF);
				}
			}
		}
		stmt.close();
		conn.close();
		return allConfs;
	}

	@Override
	public List<ArrayList<String>> getCLCETLConfs()
			throws ClassNotFoundException, SQLException {
		List<ArrayList<String>> allConfs = new ArrayList<ArrayList<String>>();
		Connection conn = utils.getMySQLConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt
				.executeQuery("SELECT * FROM engine_customlogicchain");
		while (rs.next()) {
			ArrayList<String> TEMPLATE_INSERT_CONF = new ArrayList<String>();
			TEMPLATE_INSERT_CONF.add(0, rs.getString("template"));
			TEMPLATE_INSERT_CONF.add(1, rs.getString("title"));
			allConfs.add(TEMPLATE_INSERT_CONF);
		}
		return allConfs;
	}

	@Override
	public int getShardingNum() {
		int shardingNum = 1;
		try {
			Connection conn = utils.getMySQLConnection();
			Statement stmt = conn.createStatement();
			stmt.execute("USE etlengine");
			ResultSet rs = stmt.executeQuery("SELECT * FROM engine_sharding");

			while (rs.next()) {
				if (rs.isFirst()) {
					shardingNum = rs.getInt("num");
				} else {
					break;
				}
			}
			stmt.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("shardingNum is " + shardingNum);
		return shardingNum;
	}

	@Override
	public Map<String, String> getAllAppIds(int shardingNum, String appids,
			String ds, int folderSize, String sqlengine)
			throws ClassNotFoundException, SQLException {

		Map<String, String> appIdsFinal = new HashMap<String, String>();

		List<String> allAppIds = new ArrayList<String>();
		if (appids == null) {
			Connection conn = utils.getMySQLConnectionForAppids();
			Statement stmt = conn.createStatement();
			stmt.execute("USE cy_game");
			ResultSet rs = stmt
					.executeQuery("SELECT distinct appkey FROM appinfo WHERE status=1");

			int shardingID = utils.getShardingID();

			while (rs.next()) {
				String appkey = rs.getString("appkey");
				int appkeyHash = Math.abs(appkey.hashCode());
				if (appkeyHash % shardingNum == shardingID) {
					allAppIds.add(appkey);
				}
			}
			stmt.close();
			conn.close();
		} else {
			// 对于自己指定的各种appid，就不受sharding机制的影响
			for (String appid : appids.split(",")) {
				allAppIds.add(appid);
			}
		}

		ArrayList<String> myAppIds = new ArrayList<String>();
		myAppIds.addAll(allAppIds);

		for (String appId : myAppIds) {
			appIdsFinal.put(appId, "both");
		}
		return appIdsFinal;
	}
}
