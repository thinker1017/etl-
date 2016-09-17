package etlengine;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class Utils {
    static InputStream in;
    static Properties properties = null;
    public JedisPool pool = null;

    private static Utils utils;

    public static Utils getInstance(Properties properties) {
        if (utils == null)
            utils = new Utils(properties);
        return utils;
    }

    @SuppressWarnings("static-access")
    private Utils(Properties properties) {
        this.properties = properties;
        // JedisPoolConfig jpc = new JedisPoolConfig();
        // jpc.setMaxTotal(-1);
        // jpc.setMaxIdle(16);
        // jpc.setMaxWaitMillis(1000 * 10);
        // jpc.setTestOnBorrow(true);
        pool = new JedisPool(properties.getProperty("redisResultQueueConnectionURL"));

    }

    /**
     * Get a new Hive connection object
     * */
    public synchronized Connection getHiveConnection() throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("hiveConnectionDriver", "org.apache.hadoop.hive.jdbc.HiveDriver")); // change
                                                                                                                 // to
                                                                                                                 // use
                                                                                                                 // hiveserver
        Connection conn = DriverManager.getConnection(properties.getProperty("hiveConnectionURL"), properties.getProperty("hiveConnectionUser", ""), properties.getProperty("hiveConnectionPassword", ""));
        return conn;
    }

    /**
     * Get a new Impala connection object
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     * */
    public Connection getImpalaConnection() throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("impalaConnectionDriver", "org.apache.hive.jdbc.HiveDriver"));
        Connection conn = DriverManager.getConnection(properties.getProperty("impalaConnectionURL"));
        return conn;
    }

    /**
     * Get a new MySQL connection
     * **/
    public Connection getMySQLConnection() throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("mySQLConnectionDriver"));
        Connection conn = DriverManager.getConnection(properties.getProperty("mySQLConnectionURL"), properties.getProperty("mySQLConnectionUser"), properties.getProperty("mySQLConnectionPassword"));
        return conn;
    }

    public String getLMAppids() {
        return properties.getProperty("LMappids");
    }

    /**
     * Get a new MySQL connection
     * **/
    public Connection getMySQLConnectionForAppids() throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("mySQLConnectionForAppidsDriver"));
        Connection conn = DriverManager.getConnection(properties.getProperty("mySQLConnectionForAppidsURL"), properties.getProperty("mySQLConnectionForAppidsUser"), properties.getProperty("mySQLConnectionForAppidsPassword"));
        return conn;
    }

    /**
     * Get a new Phoenix connection
     * **/
    public synchronized Connection getPhoenixConnection() throws ClassNotFoundException, SQLException {
        Class.forName(properties.getProperty("PhoenixConnectionDriver"));
        DriverManager.setLoginTimeout(3);
        Connection phoenixConnection = DriverManager.getConnection(properties.getProperty("PhoenixConnectionURL"), properties.getProperty("PhoenixConnectionUser"), properties.getProperty("PhoenixConnectionPassword"));
        return phoenixConnection;
    }

    public String getName() {
        return properties.getProperty("ETLNAME", "No ETL Name");
    }

    public int sendToQueue(String data) {
        Jedis jedis = pool.getResource();
        try {
            jedis.rpush(properties.getProperty("redisResultQueueName"), data);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                pool.returnResource(jedis);
            }
        }
        return 0;
    }

    public String getRedisResultQueueConnectionURL() {
        return properties.getProperty("redisResultQueueConnectionURL", "localhost:6379");
    }

    public String getRedisResultQueueName() {
        return properties.getProperty("redisResultQueueName", "resultsqueue");
    }

    public int getShardingID() {
        String shardingID = properties.getProperty("shardingID", "0");
        System.out.println("shardingID is " + shardingID);
        return Integer.parseInt(shardingID);
    }

    public static synchronized Connection getPrestoConnection() throws SQLException, ClassNotFoundException {
        // Class.forName("com.facebook.presto.jdbc.PrestoDriver");
        Connection prestoConnection = DriverManager.getConnection(properties.getProperty("PrestoConnectionURL"), properties.getProperty("PrestoConnectionUser"), properties.getProperty("PrestoConnectionPassword"));
        prestoConnection.setCatalog("hive");
        System.out.println("get Presto Connection!");
        return prestoConnection;
    }

    public static Map<String, String> getTrackSystemSettings(String appid, String ds) throws SQLException {
        Map<String, String> settings = new HashMap<String, String>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        // Statement stmt2 = null;
        // ResultSet rs2 = null;

        try {
            Class.forName(properties.getProperty("mySQLConnectionForAppidsDriver"));
            conn = DriverManager.getConnection(properties.getProperty("mySQLConnectionForAppidsURL"), properties.getProperty("mySQLConnectionForAppidsUser"), properties.getProperty("mySQLConnectionForAppidsPassword"));
            stmt = conn.createStatement();
            stmt.execute("use track");
            rs = stmt.executeQuery("select install_day, click_day, ci_day, normalclick, normalinstall, cilag from setting_history where appkey='" + appid + "' and ds=date_sub('" + ds + "', interval 1 day) ");

            while (rs.next()) {
                settings.put("installsubdays", rs.getString("install_day"));
                settings.put("clicksubdays", rs.getString("click_day"));
                settings.put("normalclicknumbers", rs.getString("normalclick"));
                settings.put("normalinstallnumbers", rs.getString("normalinstall"));
                settings.put("cilag", rs.getString("cilag"));
                settings.put("ciday", rs.getString("ci_day"));
            }

            // stmt2 = conn.createStatement();
            // rs2 =
            // stmt2.executeQuery("select b.channelid from app a, campaign b, channel c "
            // +
            // "where a.id=b.app and b.channel=c.id and c.name='adwords' and a.appkey='"
            // + appid + "'");
            //
            // StringBuffer channelResultString = new StringBuffer();
            // String channelList = null;
            // while(rs2.next()){
            // channelResultString =
            // channelResultString.append("'").append(rs2.getString("channelid")).append("',");
            // }
            //
            // if(channelResultString.length()>0)
            // channelList = channelResultString.substring(0,
            // channelResultString.length() - 1);
            //
            // settings.put("specialchannelid", channelList);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            conn.close();
            stmt.close();
            // stmt2.close();
            rs.close();
            // rs2.close();
        }
        return settings;
    }

}
