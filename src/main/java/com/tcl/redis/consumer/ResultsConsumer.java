package com.tcl.redis.consumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.tcl.conf.ETLConfigCtl;
import com.tcl.conf.GameConfigCtl;
import com.tcl.conf.MarketConfigCtl;

import etlengine.Utils;//.Utils;

public class ResultsConsumer {

	private Connection conn = null;
	private Statement stmt = null;
	private Utils utils;

	public ResultsConsumer(Utils utils) throws IOException {
		this.utils = utils;
	}

	public void renewStmt() throws ClassNotFoundException, SQLException {
		System.out.println("Renewing Connection...");
		conn = utils.getPhoenixConnection();
		conn.setAutoCommit(true);
		this.stmt = conn.createStatement();
	}

	public void releaseSource() throws SQLException {
		this.stmt.close();
		this.conn.close();
	}

	public Statement getStmt() {
		return this.stmt;
	}

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {
		ETLConfigCtl ecc = new GameConfigCtl("/home/reyun/engine/settings.properties");
		Utils utils = Utils.getInstance(ecc.getProperties());
		JedisPool jedisPool=utils.pool;

		Jedis jedis = jedisPool.getResource();

		ResultsConsumer rc = new ResultsConsumer(utils);
		rc.renewStmt();

		try {
			while (true) {
				List<String> message = jedis.blpop(0,
						utils.getRedisResultQueueName()); // note here
				String[] messageArray = message.get(1).split("\000");
				if (messageArray.length >= 2) {
					// System.out.println("Valid message: " + message +
					// " And now process it!"); // now news is good news
					String sql = messageArray[0];
					Statement stmt = rc.getStmt();
					try {
						if (stmt == null || stmt.isClosed()) {
							rc.renewStmt();
							stmt = rc.getStmt();
						}
						System.out.println("Execute: " + sql);
						stmt.execute(sql);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					System.out.println("Invalid message: " + message);
				}
			}
		} finally {
			jedisPool.returnResource(jedis);
			rc.releaseSource();
		}

	}

}
