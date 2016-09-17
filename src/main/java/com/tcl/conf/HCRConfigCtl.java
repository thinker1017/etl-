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

import com.tcl.util.FileSystemUtils;

public class HCRConfigCtl extends ETLConfigCtl {

	public HCRConfigCtl(String propertiesFilePath) throws IOException {
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
				if (notIn(appkey)) {
					int appkeyHash = Math.abs(appkey.hashCode());
					if (appkeyHash % shardingNum == shardingID) {
						allAppIds.add(appkey);
					}
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

		// 做一次过滤，如果对应产品的ds这天的数据没有，那么将其移除，防止空耗计算资源
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			ArrayList<String> myAppIds = new ArrayList<String>();
			myAppIds.addAll(allAppIds);

			float result = 0;

			for (String appId : myAppIds) {
				String dir = "/user/hive/warehouse/rawdata/events2/" + appId
						+ "_" + ds;
				Path p = new Path(dir);
				if (!fs.exists(p)) {
					System.out.println("Removing " + appId);
					System.out.println("The Path is " + p.toString());
					allAppIds.remove(appId);
				} else {
					result = FileSystemUtils.getFolderLen(fs, p);
					System.out.println("FolderSize---------" + appId
							+ "-----------------------" + result / 1024 / 1024);
					appIdsFinal.put(appId, "allhive");
					if (result <= (1024 * 1024 * 15)) {
						System.out.println("This appid: " + appId + "is using presto & hive");
						appIdsFinal.put(appId, "both");
					}
					// if (result >= (1024 * 1024 * folderSize)
					// && !sqlengine.equals("allhive")) {
					// appIdsFinal.put(appId, "prestohive");
					// }
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return appIdsFinal;
	}

	private boolean notIn(String appkey) {
		// --fake data start--
		List<String> notin = new ArrayList<String>();
		notin.add("bc29df40f3f5be5af18de432cb536ef0");
		notin.add("4797442a6471ca43378dce6f9d1f689d");
		notin.add("154aaf11dd89434e84b6967d32d7e3d3");
		notin.add("76994370e101ece46e4f027981f57f06");
		notin.add("0179e7d82491fd9c32b1784dc0978d11");
		notin.add("95a5b85ec8492ebcfc7aec053c0e3e17");
		notin.add("5893e387d967aab690efb36ca4aedcca");
		notin.add("9c6adb3defc83155099a84c659b26295");
		notin.add("899d87889bfb83b53d0768f8e4d0a95d");
		notin.add("323e7b0078abf037b46d3e6a4e86f2e6");
		notin.add("ee8c8325ef4162125ee5a399bab5c001");
		notin.add("ad376ac9ef5501d374677c3821576891");
		notin.add("63df8e144464abf9b9bf9e444520cd96");
		notin.add("e876eab218daa34e07daf7932d5d0644");
		notin.add("c72b3ad19e9d61d2b151af5747b06ae4");
		notin.add("c79b61632bf2b56b3460a99f4fe988b1");
		notin.add("839b6ba7af9aa52bd7de53e597169411");
		notin.add("fe74b288f8c6bb4f97dbff741b3c528d");
		notin.add("e305b347d71059dd86c39e4ae5485706");
		notin.add("b8013e0205958f8642c33f1163fcbeda");
		notin.add("b32d476ac9c08c0dfc50a6bab210c127");
		notin.add("4daf45679c84736d60c2ea3d8a50ba04");
		notin.add("c986e48c3b23d2f815c96d863e565ac9");
		notin.add("adbf8f8874b49b4f9b7893150bac92da");
		notin.add("b0362568cc423a05f17298bd78c500b0");
		notin.add("739fe4317ec7fa833f709fbbd474eadb");
		notin.add("cbe8c2a4d2d45b4356d1a8cbf5c16fd9");
		notin.add("ce911d6c74ead24f18cef0f4d40947d0");
		notin.add("33c2a371f4d24c4bc6b2a82a834bf586");
		notin.add("fc3c0490316cb59eae19fcc82db5d69f");
		notin.add("33fd21c36c2fed04bc2230ed8a6cd929");
		notin.add("9775340312bcc27004f2fdc96b4ea374");
		notin.add("bab6ea5af043a3a829b1f7d2f7bc80ce");
		notin.add("5436b839140366e8e6fc577b2c43843a");
		notin.add("983eba2c96327ad91b08fda9187a717c");
		notin.add("c015d5cd42d518b87549d7e2a2f9be08");
		notin.add("9143838bbb1177a9d2efcc4a14f1bf80");
		notin.add("29a4ba40a4d98e97ac7ede7e3884ca5b");
		notin.add("98755ded023ca794a3031a5a5fa53582");
		notin.add("cbb589844e7bb70383b31d5562a33b36");
		notin.add("afa24821587b673bc4949c41927fdf18");
		notin.add("dc0afdb95bf5aaef1af1315f6ce942e3");
		notin.add("acd58b4086f442d7f311717e91106bc5");
		notin.add("2ed4592e0221e6259e8f93563ae95620");
		notin.add("a0031298829098425d53c3d65e6c717b");
		notin.add("4aae0a8bbf425d5db3bdace8bf301f71");
		notin.add("cbc4fc952303261ad164bd2c26e81aaf");
		notin.add("9b4d7da7e41740fb63d38a494bd4327a");
		notin.add("12864692a16b404078a0600af981b112");
		notin.add("d484b24368d99de0f5609165b7c3a8f9");

		notin.add("6c3ac6b75389329b6018c331cf2920ac");
		notin.add("df35ffa8e6dcf1dfe86a80e735e5febf");
		notin.add("df35ffa8e6dcf1dfe86a80e735e5febf");
		notin.add("181958b97c2beedeaa975363b104c87a");
		notin.add("a8fd0c2cc4201bffe80de778fed09a1e");
		notin.add("b58f962e23777b60afb046749c279293");
		notin.add("593d9ab70ec8dce6f4147886b4a71943");
		notin.add("7314228f27897077dce94235a8c0976d");
		notin.add("e8d669f68bb92482060c799f2ee025bd");
		// --fake data end--

		// --WTF start--
		notin.add("525f8c3713fafcc96b70d20ba98f819c");
		notin.add("15560bb78e4ebeff3227d194bf777186");
		notin.add("0da195d69c0807b156011a60bb09fe2f");
		notin.add("36a1a0d61476f15d8737549adee7785a");
		notin.add("c6519f7793adca12d1ac0736b0bd3d6e");
		notin.add("b26039998847ec5ddae9eb658baea180");
		notin.add("3a250a844d0cf6e100332031c4b61ebf");
		// --WTF end--
		
//		notin.add("4fa643c798cdae71ebd8e2d7ef0d78af");
		
		if (notin.contains(appkey.toLowerCase())) {
			return false;
		}

		return true;
	}
}
