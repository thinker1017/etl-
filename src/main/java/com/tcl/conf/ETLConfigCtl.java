package com.tcl.conf;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import etlengine.Utils;

public abstract class ETLConfigCtl {
	Utils utils;
	Properties properties = new Properties();

	public ETLConfigCtl(String propertiesFilePath) throws IOException {

		BufferedInputStream in = new BufferedInputStream(new FileInputStream(
				propertiesFilePath));
		properties.load(in);
		this.utils = this.getUtils();
	}

	/**
	 * Get all the Base ETL HQL templates and the configurations
	 * **/
	public abstract List<ArrayList<String>> getBaseETLConfs()
			throws ClassNotFoundException, SQLException;

	/**
	 * Get all the HQL templates and the configurations
	 * **/
	public abstract List<ArrayList<String>> getCumulativesETLConfs()
			throws ClassNotFoundException, SQLException;

	/**
	 * Get all the HQL templates and the configurations
	 * **/
	public abstract List<ArrayList<String>> getConceptionETLConfs()
			throws ClassNotFoundException, SQLException;

	/**
	 * Get all the Report ETL HQL templates and the configuration for how to
	 * store them in the business database.
	 * **/
	public abstract List<ArrayList<String>> getReportETLConfs(String reports)
			throws ClassNotFoundException, SQLException;

	public abstract List<ArrayList<String>> getCLCETLConfs()
			throws ClassNotFoundException, SQLException;

	public abstract int getShardingNum();

	/**
	 * If not specified, get all the appids from database, else, just use the
	 * specified appids
	 * **/
	public abstract Map<String, String> getAllAppIds(int shardingNum, String appids,
			String ds, int folderSize, String sqlengine) throws ClassNotFoundException, SQLException;

	public Properties getProperties() {
		return properties;
	}

	public Utils getUtils() {
		Utils utils = Utils.getInstance(this.properties);
		return utils;
	}

}
