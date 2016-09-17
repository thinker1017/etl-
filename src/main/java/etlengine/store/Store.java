package etlengine.store;

import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;

public interface Store {
//	public boolean execute_sql(String delete_sql) throws Exception;

	public boolean execute_sql(String delete_sql, String platform, String appid, String ds)
			throws Exception;

	//封装sql，并且执行
	public boolean save(ArrayList<HashMap<String, String>> newRS, String table,
			JSONArray columns, JSONArray column_types, String platform, String appid, String ds) throws Exception;
}
