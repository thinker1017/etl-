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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LMGameConfigCtl extends ETLConfigCtl {

	public LMGameConfigCtl(String propertiesFilePath) throws IOException {
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

			int shardingID = utils.getShardingID();

			String appkeyFromProperties = utils.getLMAppids();
			String[] appkeys = appkeyFromProperties.split(",");
			for (String appkey : appkeys) {
				if (notIn(appkey)) {
					int appkeyHash = Math.abs(appkey.hashCode());
					if (appkeyHash % shardingNum == shardingID) {
						allAppIds.add(appkey);
					}
				}
			}
		} else {
			// 对于自己指定的各种appid，就不受sharding机制的影响
			for (String appid : appids.split(",")) {
				allAppIds.add(appid);
			}
		}

		// 做一次过滤，如果对应产品的ds这天的数据没有，那么将其移除，防止空耗计算资源
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			ArrayList<String> myAppIds = new ArrayList<String>();
			myAppIds.addAll(allAppIds);

			for (String appId : myAppIds) {
				String dir = "/user/hive/warehouse/rawdata/events2/" + appId
						+ "_" + ds;
				Path p = new Path(dir);
				if (!fs.exists(p)) {
					System.out.println("Removing " + appId);
					System.out.println("The Path is " + p.toString());
					allAppIds.remove(appId);
				} else {
					appIdsFinal.put(appId, "both");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return appIdsFinal;
	}

	private boolean notIn(String appkey) {
		return false;
	}
}
