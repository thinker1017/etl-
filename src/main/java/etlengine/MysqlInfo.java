package etlengine;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class MysqlInfo {
	
	static InputStream in;
	static Properties properties = null;
	static {
		try {
			in = new BufferedInputStream(new FileInputStream("/opt/engine/settings.properties"));
			properties = new Properties();
			properties.load(in);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static class Constant{
		public static List<JedisShardInfo> readshards = new ArrayList<JedisShardInfo>();
		static{
			for(String item : properties.getProperty("readRedis").split(",")){
				String[] ipport = item.split(":");
				readshards.add(new JedisShardInfo(ipport[0], Integer.parseInt(ipport[1])));
			}
		};
		
		public static String MYSQL_DRIVER=properties.getProperty("mySQLConnectionDriver", "com.mysql.jdbc.Driver");
	}

	private static Map<String, String> map = new HashMap<String, String>();
	private static ShardedJedisPool pool = null;
	static{
		GenericObjectPoolConfig redisConfig = new GenericObjectPoolConfig();
		pool = new ShardedJedisPool(redisConfig, Constant.readshards);
	}
	
	
	/*
	 * 获取非连接池连接
	 */
	public static Connection getConnectionInfo(String appkey){
		Connection connection = null;
		try{
			String vString = getMysqlInfoForApp(appkey);
			if(!isTranslateDB(vString)){
				connection = getConnectionJDBC(appkey,vString);
				//处理逻辑
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return connection;
	}
	
	/*
	 * 获取某个appkey对应的数据库连接信息
	 */
	public static String getMysqlInfoForApp(String appkey){
		String key = "appkey_mysql_base";
		if(pool!=null){
		    GenericObjectPoolConfig redisConfig = new GenericObjectPoolConfig();
			pool = new ShardedJedisPool(redisConfig, Constant.readshards);
		}
		ShardedJedis jedis = pool.getResource();
		//"appkey_mysql,appkey,(mysqlurl;username;password;0)"
		String value = jedis.hget(key, appkey);
		pool.returnResource(jedis);
		if(value!=null && !"".equals(value)){
			String[] nums = value.split(";");
			if(nums.length==4){
				if(map==null){
					map = new HashMap<String, String>();
				}
				map.put(appkey, value);//记录，防止崩溃
				return value;
			}
		}else{//如果redis中没有获取到appkey对应的mysql信息，从原有的map中获取
			if(map==null){
				map = new HashMap<String, String>();
			}
			return map.get(appkey);
		}
		return null;
	}
	
	/**
	 * false : 未进行数据库迁移  true：数据库迁移中
	 * @param mysqlinfo
	 * @return
	 */
	public static boolean isTranslateDB(String mysqlinfo){
		
		String[] nums = mysqlinfo.split(";");
		if(nums.length==4){
			String value = nums[3];
			if(value==null || "0".equals(value)){
				return false;
			}else if("1".equals(value)){
				return true;
			}
		}
		return false;
	}
	
	public static Connection getConnectionJDBC(String appkey,String val){
		
		Connection connection = null;
		try{
			if(val!=null){
				String[] info = val.split(";");
				if(info.length==4){
					
					String driverUrl = "jdbc:mysql://"+info[0]+"?useUnicode=true&characterEncoding=utf-8&useOldAliasMetadataBehavior=true";
					Class.forName(Constant.MYSQL_DRIVER).newInstance();
					connection = DriverManager.getConnection(driverUrl,info[1],info[2]);
					connection.setAutoCommit(false);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return connection;
		
	}
	
}
