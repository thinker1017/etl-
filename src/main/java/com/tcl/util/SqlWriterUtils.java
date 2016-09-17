package com.tcl.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlWriterUtils {
	private static SqlWriterUtils sqlWriter = new SqlWriterUtils();;
	private static Map<String, FileWriter> writers = new ConcurrentHashMap<String, FileWriter>();

	private SqlWriterUtils() {
	}

	private static String generatorFilePath(String appid, String ds) {
		File dspath = new File("./sqllogs/" + ds);
		dspath.mkdirs();
		return "./sqllogs/" + ds + "/" + appid;
	}

	public static SqlWriterUtils getInstance() {
		return sqlWriter;
	}

	public void write(String appid, String ds, String sql) {
		try {
			FileWriter fw = writers.get(generatorFilePath(appid, ds));
			if (fw == null) {
				fw = new FileWriter(new File(generatorFilePath(appid, ds)),
						true);
				writers.put(generatorFilePath(appid, ds), fw);
			}
			fw.write(sql + "\r\n");

		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	public void close() {
		Iterator<String> iter = writers.keySet().iterator();

		while (iter.hasNext()) {
			String key = iter.next();
			try {

				writers.get(key).close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < 10; i++) {
			SqlWriterUtils.getInstance().write("appid" + i, "2013-04-01",
					"sql" + i);
		}
		SqlWriterUtils.getInstance().close();
	}
}
