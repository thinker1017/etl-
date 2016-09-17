package etlengine.store;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;

import com.tcl.util.SqlWriterUtils;

import etlengine.Utils;

public class PhoenixStore implements Store {
	private Utils utils;

	public PhoenixStore(Utils utils) {
		this.utils = utils;
	}

	// public boolean execute_sql(String sql) throws Exception {
	// utils.sendToQueue(sql + "\000" + "phoenix"); // New way to store results
	// return true;
	// }

	public boolean execute_sql(String sql, String platform, String appid,
			String ds) // store lm results
			throws Exception {
		System.out.println("now the platform is --------------------->"
				+ platform);
		System.out.println("appid ----[" + appid + "]----ds----[" + ds + "]----sql----[" + sql + "]");
		if ("lm".equalsIgnoreCase(platform) || "qikugame".equalsIgnoreCase(platform)) {
			System.out.println("platform----------------------->" + platform);
			if (sql != null && !("".equals(sql))) {
				SqlWriterUtils.getInstance().write(appid, ds, sql + ";");
			}
		} else
			utils.sendToQueue(sql + "\000" + "phoenix"); // New way to store
															// results
		return true;
	}

	public boolean save(ArrayList<HashMap<String, String>> newRS, String table,
			JSONArray columns, JSONArray column_types, String platform,
			String appid, String ds) throws Exception {
		System.out
				.println("Trying to insert to Phoenix As ArrayList of HashMap !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
		for (HashMap<String, String> eachRecord : newRS) {
			String insert_sql = "UPSERT INTO " + table + " ";
			StringBuilder columnsdesc = new StringBuilder();
			StringBuilder valuesdesc = new StringBuilder();
			columnsdesc.append("(");
			valuesdesc.append("(");
			for (int i = 0; i < columns.length(); i++) {
				String column = columns.getString(i);
				String column_type = column_types.getString(i);
				String value = eachRecord.get(column);

				if (value == "") {
					value = "unknown";
				}

				if (i != 0) {
					columnsdesc.append(",");
					valuesdesc.append(",");
				}
				columnsdesc.append(column);
				if (column_type.equalsIgnoreCase("integer")
						|| column_type.equalsIgnoreCase("bigint")
						|| column_type.equalsIgnoreCase("tinyint")
						|| column_type.equalsIgnoreCase("float")
						|| column_type.equalsIgnoreCase("double")) {
					if (value.equals("0.0")) {
						value = "0"; // 0.0 cannot insert into decimal column,
										// but 0 is valid
					}
					if (value.contains("E") || value.contains("e")) {
						BigDecimal db = new BigDecimal(value);
						value = db.toPlainString();
					}
					valuesdesc.append(String.format("%s", value));
				} else {
					valuesdesc.append(String.format("'%s'",
							value.replace("'", "\\'")));
				}
			}
			columnsdesc.append(")");
			valuesdesc.append(")");

			insert_sql = insert_sql + columnsdesc.toString() + "VALUES"
					+ valuesdesc.toString();
			Long startTime = System.currentTimeMillis();
//			SqlWriterUtils.getInstance().write(appid, ds, insert_sql + ";");
			this.execute_sql(insert_sql, platform, appid, ds);
			Long endTime = System.currentTimeMillis();
			Long deltaTime = (endTime - startTime);
			System.out.println("Phoenix SQL " + insert_sql
					+ " insert time is ----------> " + deltaTime
					+ "ms <----------");
		}
		return true;
	}
}
