package etlengine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.tcl.conf.ETLConfigCtl;
import com.tcl.mail.MailContentUtils;
import com.tcl.util.RenderUtils;

import etlengine.store.PhoenixStore;
import etlengine.store.Store;

public class HQLThread implements Runnable {

	private String appid;
	private Map<String, String> ENV = new HashMap<String, String>(); // Used to
																		// render
																		// the
																		// template
	private Connection hiveconnection = null;
	private Connection prestoconnection = null;
	private ArrayList<String> BaseETL_CONF = null;
	private ArrayList<String> cumulativesETL_CONF = null;
	private ArrayList<String> conceptionETLConf = null;
	private ArrayList<String> TEMPLATE_INSERT_CONF = null; // ReportETL Conf
	private Utils utils;
	private ArrayList<String> CLCETLConf = null;
	private String sqlengine;
	private String platform;
	private String ds;

	public HQLThread(Map<String, String> ENV, String appid, ETLConfigCtl ecc,
			String sqlengine, String platform, String ds) {
		this.ENV = ENV;
		this.appid = appid;
		this.utils = ecc.getUtils();
		this.sqlengine = sqlengine;
		this.platform = platform;
		this.ds = ds;
	}

	public void run() {
		Long startTime = System.currentTimeMillis();
		String rendered_sql = null;
		String[] rendered_sqls = null;
		String hqltemplate = null;
		Statement stmt = null;
		@SuppressWarnings("unused")
		String clcCMD = null;

		String title = null;
		@SuppressWarnings("unused")
		String level;
		@SuppressWarnings("unused")
		String state = "0";
		int prestoConnectionFlag = 0;

		try {
			this.setHiveconnection(utils.getHiveConnection());
			stmt = this.hiveconnection.createStatement();

			if (BaseETL_CONF != null) {
				hqltemplate = this.BaseETL_CONF.get(0);
				title = this.BaseETL_CONF.get(1);
				level = "0";
				try {
					rendered_sql = RenderUtils.render(hqltemplate, ENV);
					rendered_sqls = RenderUtils.split(rendered_sql, ";");
					if (rendered_sqls != null) {
						for (String unit_sql : rendered_sqls) {
							stmt.execute(unit_sql.trim());
						}
					}
				} catch (Exception e) {
					try {
						rendered_sql = RenderUtils.render(hqltemplate, ENV);
						rendered_sqls = RenderUtils.split(rendered_sql, ";");
						if (rendered_sqls != null) {
							for (String unit_sql : rendered_sqls) {
								stmt.execute(unit_sql.trim());
							}
						}
					} catch (Exception e2) {
						e2.printStackTrace();
						System.out
								.println("***************sql's retry time is 1!");
						MailContentUtils.addMailContent(e2, this.appid,
								rendered_sql, 1, title);
					}
				}
			}
			// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

			if (cumulativesETL_CONF != null) {
				hqltemplate = this.cumulativesETL_CONF.get(0);
				title = this.cumulativesETL_CONF.get(1);
				level = "1";

				try {
					rendered_sql = RenderUtils.render(hqltemplate, ENV);
					rendered_sqls = RenderUtils.split(rendered_sql, ";");
					if (rendered_sqls != null) {
						for (String unit_sql : rendered_sqls) {
							stmt.execute(unit_sql.trim());
						}
					}
				} catch (Exception e) {
					try {
						rendered_sql = RenderUtils.render(hqltemplate, ENV);
						rendered_sqls = RenderUtils.split(rendered_sql, ";");
						if (rendered_sqls != null) {
							for (String unit_sql : rendered_sqls) {
								stmt.execute(unit_sql.trim());
							}
						}
					} catch (Exception e2) {
						e2.printStackTrace();
						System.out
								.println("***************sql's retry time is 1!");
						MailContentUtils.addMailContent(e2, this.appid,
								rendered_sql, 1, title);
					}
				}
			}

			if (this.conceptionETLConf != null) {
				hqltemplate = this.conceptionETLConf.get(0);
				title = this.conceptionETLConf.get(1);
				level = "2";

				try {
					rendered_sql = RenderUtils.render(hqltemplate, ENV);
					rendered_sqls = RenderUtils.split(rendered_sql, ";");
					if (rendered_sqls != null) {
						for (String unit_sql : rendered_sqls) {
							stmt.execute(unit_sql.trim());
						}
					}
				} catch (Exception e) {
					try {
						rendered_sql = RenderUtils.render(hqltemplate, ENV);
						rendered_sqls = RenderUtils.split(rendered_sql, ";");
						if (rendered_sqls != null) {
							for (String unit_sql : rendered_sqls) {
								stmt.execute(unit_sql.trim());
							}
						}
					} catch (Exception e2) {
						e2.printStackTrace();
						System.out
								.println("***************sql's retry time is 1!");
						MailContentUtils.addMailContent(e2, this.appid,
								rendered_sql, 1, title);
					}
				}
			}

			// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			if (TEMPLATE_INSERT_CONF != null) { // Report ETL
				hqltemplate = this.TEMPLATE_INSERT_CONF.get(0);
				String insertConf = this.TEMPLATE_INSERT_CONF.get(1);
				title = TEMPLATE_INSERT_CONF.get(2);
				level = "4";
				int retrytime = 0;

				// 对应多种可能的引擎模式，增加converttemplate,
				// hql完整形式"@@$hive@@ show tables @@$presto@@ show tables @@spark@@ show tables";
				String[] differenttemplates = null;

				// 如果hqltemplate中没有@@符号，则默认使用hqltemplate的sql
				String converttemplate = hqltemplate;

				// 对于指定sql引擎的模式，如果为presto，则只跑带presto的sql；如果为both，那么智能检测跑hive or
				// presto；如果为hive，那么全部sql均用hive
				if (this.sqlengine.equals("presto")) {
					System.out.println("1" + this.sqlengine);
					if (hqltemplate.contains("@@")) {
						differenttemplates = hqltemplate.split("@@");
						converttemplate = differenttemplates[4];
						this.setPrestoconnection(Utils.getPrestoConnection());
						stmt = this.prestoconnection.createStatement();
						prestoConnectionFlag = 1;
					} else {
						return;
					}
				} else if (this.sqlengine.equals("both")) {
					System.out.println("2" + this.sqlengine);
					if (hqltemplate.contains("@@")) {
						differenttemplates = hqltemplate.split("@@");
						converttemplate = differenttemplates[4];
						this.setPrestoconnection(Utils.getPrestoConnection());
						stmt = this.prestoconnection.createStatement();
						prestoConnectionFlag = 1;
					} else
						converttemplate = hqltemplate;
				} else if (this.sqlengine.equals("prestohive")) {
					System.out.println("3" + this.sqlengine);
					if (hqltemplate.contains("@@")) {
						differenttemplates = hqltemplate.split("@@");
						converttemplate = differenttemplates[2];
					} else
						return;
				} else if (this.sqlengine.equals("onlyhive")) {
					System.out.println("4" + this.sqlengine);
					if (hqltemplate.contains("@@")) {
						return;
					} else
						converttemplate = hqltemplate;
				} else if (this.sqlengine.equals("allhive")) {
					System.out.println("5" + this.sqlengine);
					if (hqltemplate.contains("@@")) {
						differenttemplates = hqltemplate.split("@@");
						converttemplate = differenttemplates[2];
					} else
						converttemplate = hqltemplate;
				}

				try {
					rendered_sql = RenderUtils.render(converttemplate, ENV);
					ResultSet rs = stmt.executeQuery(rendered_sql);
					this.post_sql_execution(rs, insertConf, this.ENV);
				} catch (Exception e) {
					e.printStackTrace();
					System.out
							.println("SQL title : "
									+ title
									+ " by Using this sqlengine====> "
									+ this.sqlengine
									+ " has failed, now trying to rerun this sql by hive");
					retrytime = 1;
					try {
						if (hqltemplate.contains("@@")) {
							differenttemplates = hqltemplate.split("@@");
							converttemplate = differenttemplates[2];
						} else
							converttemplate = hqltemplate;
						this.setHiveconnection(utils.getHiveConnection());
						stmt = this.hiveconnection.createStatement();
						rendered_sql = RenderUtils.render(converttemplate, ENV);
						ResultSet rs = stmt.executeQuery(rendered_sql);
						this.post_sql_execution(rs, insertConf, this.ENV);
					} catch (Exception e2) {
						e2.printStackTrace();
						MailContentUtils.addMailContent(e2, this.appid,
								rendered_sql, retrytime, title);
					}
				}
			}

			// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			if (CLCETLConf != null) {
				hqltemplate = this.CLCETLConf.get(0);
				title = this.CLCETLConf.get(1);
				level = "1";

				try {
					rendered_sql = RenderUtils.render(hqltemplate, ENV);
					rendered_sqls = RenderUtils.split(rendered_sql, ";");
					if (rendered_sqls != null) {
						for (String unit_sql : rendered_sqls) {
							stmt.execute(unit_sql.trim());
						}
					}
				} catch (Exception e) {
					try {
						rendered_sql = RenderUtils.render(hqltemplate, ENV);
						rendered_sqls = RenderUtils.split(rendered_sql, ";");
						if (rendered_sqls != null) {
							for (String unit_sql : rendered_sqls) {
								stmt.execute(unit_sql.trim());
							}
						}
					} catch (Exception e2) {
						e2.printStackTrace();
						System.out
								.println("***************sql's retry time is 1!");
						MailContentUtils.addMailContent(e2, this.appid,
								rendered_sql, 1, title);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			// 通过邮件发送此信息给相关人员
			state = "1";
			BigEngine.addMailContent(
					"Error in creating statement, plz check the connection",
					e.toString());
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				this.hiveconnection.close();
				if (this.prestoconnection != null && prestoConnectionFlag == 1) {
					this.prestoconnection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			Long endTime = System.currentTimeMillis();
			Long deltaTime = (endTime - startTime) / 1000;
			System.out.println("Executed: [" + title
					+ "] IN ------------------------->" + deltaTime
					+ "Seconds!");
		}
	}

	// Currently save the results directly, better if put these results to a
	// Queue server and process them on the other end of the queue
	private void post_sql_execution(ResultSet rs, String insertConf,
			Map<String, String> ENV) throws Exception {
		ArrayList<String> insertConfs = new ArrayList<String>();
		if (insertConf.startsWith("[")) {
			JSONArray jsonArray = new JSONArray(insertConf);
			for (int i = 0; i < jsonArray.length(); i++) {
				insertConfs.add(jsonArray.getString(i));
			}
		} else if (insertConf.startsWith("{")) {
			insertConfs.add(insertConf);
		}
		ArrayList<HashMap<String, String>> newRS = null;

		for (String everyInsertConf : insertConfs) {
			JSONObject jsonObject = new JSONObject(everyInsertConf);
			@SuppressWarnings("unused")
			String store = jsonObject.getString("store");
			String table = jsonObject.getString("table");

			JSONArray columns = jsonObject.getJSONArray("columns");
			JSONArray column_types = jsonObject.getJSONArray("column_types");
			// ////如果没有将结果组织到新的数据结构，就组织一次//////
			if (newRS == null) {
				newRS = new ArrayList<HashMap<String, String>>();
				while (rs.next()) {
					HashMap<String, String> currentRow = new HashMap<String, String>();
					for (int i = 0; i < columns.length(); i++) {
						String column = columns.getString(i);
						String column_type = column_types.getString(i);
						String value = rs.getString(i + 1);
						if (value == "") {
							value = "unknown";
						}
						if (column_type.equalsIgnoreCase("integer")
								|| column_type.equalsIgnoreCase("bigint")
								|| column_type.equalsIgnoreCase("tinyint")
								|| column_type.equalsIgnoreCase("float")
								|| column_type.equalsIgnoreCase("double")) {
							if (value.equals("0.0")) {
								value = "0"; // 0.0 cannot insert into decimal
												// column, but 0 is valid
							}
						}
						currentRow.put(column, value);
					}

					newRS.add(currentRow);
				}

			}
			// ////如果没有将结果组知道新的数据结构，就组织一次//////

			Store insert_store = null;
			insert_store = new PhoenixStore(utils);

			if (jsonObject.has("delete_sql")) { // 如果有对应的删除要求，进行以下操作
				String delete_sql_template = jsonObject.getString("delete_sql");
				ENV.put("table", table);
				String rendered_delete_sql = RenderUtils.render(
						delete_sql_template, ENV);
				// 龙马特殊需求，sql需要单独存储
				insert_store.execute_sql(rendered_delete_sql, this.platform,
						this.appid, this.ds);
			}

			insert_store.save(newRS, table, columns, column_types,
					this.platform, this.appid, this.ds);
		}
	}

	public ArrayList<String> getCLCETLConf() {
		return CLCETLConf;
	}

	public void setCLCETLConf(ArrayList<String> cLCETLConf) {
		CLCETLConf = cLCETLConf;
	}

	// Following: getters and setters
	public Connection getHiveconnection() {
		return hiveconnection;
	}

	public void setHiveconnection(Connection hiveconnection) {
		this.hiveconnection = hiveconnection;
	}

	public ArrayList<String> getTEMPLATE_INSERT_CONF() {
		return TEMPLATE_INSERT_CONF;
	}

	public void setTEMPLATE_INSERT_CONF(ArrayList<String> tEMPLATE_INSERT_CONF) {
		TEMPLATE_INSERT_CONF = tEMPLATE_INSERT_CONF;
	}

	public ArrayList<String> getTEMPLATE_ETL_CONF() {
		return BaseETL_CONF;
	}

	public void setTEMPLATE_ETL_CONF(ArrayList<String> baseETL_CONF) {
		BaseETL_CONF = baseETL_CONF;
	}

	public ArrayList<String> getCumulativesETL_CONF() {
		return cumulativesETL_CONF;
	}

	public void setCumulativesETL_CONF(ArrayList<String> cumulativesETL_CONF) {
		this.cumulativesETL_CONF = cumulativesETL_CONF;
	}

	public ArrayList<String> getConceptionETLConf() {
		return conceptionETLConf;
	}

	public void setConceptionETLConf(ArrayList<String> conceptionETLConf) {
		this.conceptionETLConf = conceptionETLConf;
	}

	public Connection getPrestoconnection() {
		return prestoconnection;
	}

	public void setPrestoconnection(Connection prestoconnection) {
		this.prestoconnection = prestoconnection;
	}
}
