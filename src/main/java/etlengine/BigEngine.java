package etlengine;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.tcl.conf.ETLConfigCtl;
import com.tcl.conf.GameConfigCtl;
import com.tcl.conf.HCRConfigCtl;
import com.tcl.conf.LMGameConfigCtl;
import com.tcl.conf.LTConfigCtl;
import com.tcl.conf.MarketConfigCtl;
import com.tcl.conf.QikuGameConfigCtl;
import com.tcl.conf.TrackConfigCtl;
import com.tcl.mail.SimpleMailSender;
import com.tcl.util.SqlWriterUtils;

public class BigEngine {
	private String ds;
	private Integer enginethreads = null;
	private Integer hqlthreads = null;
	private String appids = null;
	private String step = null;
	private String stepend = "4";
	private String reports = null;
	private String sqlengine = "both";
	private int foldersize = 100;
	// 以下每个Conf是一个数组，数组的第一项是具体的模板配置
	private List<ArrayList<String>> baseETLConfs = null;
	private List<ArrayList<String>> cumulativesETLConfs = null;
	private List<ArrayList<String>> conceptionETLConfs = null;
	private List<ArrayList<String>> reportETLConfs = null;
	@SuppressWarnings("unused")
	private List<String> allAppIds = null;
	private Map<String, String> allAppIdsMap = null;
	private Utils utils;
	private ETLConfigCtl ecc;
	private SimpleMailSender sms;
	private String platform;

	private List<ArrayList<String>> CLCETLConfs = null;

	// 注意以下方法只适合于crontab环境下使用
	public static HashMap<String, String> mailContent = new HashMap<String, String>();

	public static synchronized void addMailContent(String key, String value) {
		mailContent.put(key, value);
	}

	private void sendMailContent() {
		if (BigEngine.mailContent.size() > 0) { // 如果有报错的情况，才发送邮件
			String subject = "ETL Exceptions For " + utils.getName() + " At "
					+ ds;
			StringBuilder sb = new StringBuilder();
			sb.append("<b>Please check the exceptions below and correct them as soon as possible: </b><br />");
			for (Entry<String, String> entry : BigEngine.mailContent.entrySet()) {
				sb.append(entry.getKey().replace("\n", "<br />") + "<br />");
				sb.append("<=========================================><br />");
				sb.append("<b>" + entry.getValue() + "</b><br /><p />");
			}
			sms.sendMail(subject, sb.toString());
		}
	}

	// 注意以上方法只适合于crontab环境下使用

	private void sendMailStart() throws ClassNotFoundException, SQLException {
		List<String> todayAppIds = new ArrayList<String>(
				this.allAppIdsMap.keySet());

		StringBuffer sb = new StringBuffer();
		sb.append(">====");
		for (String appid : todayAppIds) {
			sb.append(appid).append("==");
		}
		sb.append("==<");
		sms.sendMail(
				"ETL start ---> " + utils.getName() + ", DS is: " + this.ds,
				String.format("<b>ETL Start With %d appIds</b>",
						todayAppIds.size())
						+ ", running appids are: " + sb.toString());
	}

	private void sendMailEnd() {
		sms.sendMail("ETL end ---> " + utils.getName() + ", DS is: " + this.ds,
				"<b>ETL END</b>");
	}

	public BigEngine(String ds, Integer enginethreads, Integer hqlthreads,
			String appids, String step, String stepend, String reports,
			ETLConfigCtl ecc, String sqlengine, int foldersize, String platform)
			throws IOException {
		this.ds = ds;
		this.enginethreads = enginethreads; // 同时跑多少个产品的数据
		this.hqlthreads = hqlthreads; // 每个产品中，同时跑的HQL个数
		this.appids = appids;
		this.step = step;
		this.stepend = stepend;
		this.reports = reports;
		this.utils = ecc.getUtils();
		this.ecc = ecc;
		this.sqlengine = sqlengine;
		this.foldersize = foldersize;
		this.platform = platform;
		sms = new SimpleMailSender(ecc.getProperties());
	}

	public void pre_start(String sqlengine) throws ClassNotFoundException,
			SQLException {
		this.baseETLConfs = ecc.getBaseETLConfs();
		this.cumulativesETLConfs = ecc.getCumulativesETLConfs();
		this.reportETLConfs = ecc.getReportETLConfs(reports);
		this.conceptionETLConfs = ecc.getConceptionETLConfs();
		this.allAppIdsMap = ecc.getAllAppIds(ecc.getShardingNum(), appids, ds,
				this.foldersize, this.sqlengine);
		this.CLCETLConfs = ecc.getCLCETLConfs(); // key为appid，值为对应的JSON定义
		try {
			this.sendMailStart();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void post_start() throws SQLException {
		try {
			System.out.println("Sending Mail......");
			this.sendMailContent();
			this.sendMailEnd();
		} catch (Exception e) {
			System.out.println(String.format("Fail to send mail because [%s]",
					e.toString()));
			System.out.println("-------Mail Content--------");
			System.out.println(BigEngine.mailContent);
		}
	}

	/**
	 * Start the engine
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @throws IOException
	 * **/
	public void start() throws ClassNotFoundException, SQLException,
			InterruptedException, IOException {
		Long startTime = System.currentTimeMillis();
		List<String> appids = new ArrayList<String>(this.allAppIdsMap.keySet());
		ExecutorService exec = Executors.newFixedThreadPool(enginethreads); // 同时执行多个游戏的运算

		for (String appid : appids) {
			String realengine = "both";
			if (this.sqlengine != null)
				realengine = this.allAppIdsMap.get(appid);
			ETLEngine etlEngine = new ETLEngine(appid, this.ds, this.step, ecc,
					realengine, this.stepend, this.platform);
			System.out.println("Bigengine --------------> flat is : "
					+ this.platform);
			etlEngine.setHqlthreads(this.hqlthreads); // 每个游戏中同时执行多少个HQL语句
			etlEngine.setBaseETLConfs(baseETLConfs);
			etlEngine.setCumulativesETLConfs(cumulativesETLConfs);
			etlEngine.setReportETLConfs(reportETLConfs);
			etlEngine.setConceptionETLConfs(conceptionETLConfs);
			etlEngine.setCLCETLConfs(CLCETLConfs);
			exec.execute(etlEngine);
		}
		exec.shutdown();
		exec.awaitTermination(12, TimeUnit.HOURS);
		Long endTime = System.currentTimeMillis();
		Long deltaTime = (endTime - startTime) / 1000;
		System.out.println("All Threads Ended In " + deltaTime + " Seconds!");
	}

	public static void main(String[] args) throws ParseException,
			ClassNotFoundException, SQLException, InterruptedException,
			IOException {
		String project = null;
		String ds = null;
		Integer enginethreads = null;
		Integer hqlthreads = null;
		String appids = null;
		String step = null;
		String reports = null;
		String sqlengine = null;
		String stepend = "4";
		String platform = "track";
		int foldersize = 100;

		Options options = new Options();
		options.addOption("project", true,
				"project name : [game,track,zhaohuanshi,wau,mau,rerun]");
		options.addOption("ds", true, "date stamp");
		options.addOption("enginethreads", true, "Engine threads");
		options.addOption("hqlthreads", true, "HQL threads started for a game");
		options.addOption("appids", true, "comma-seperated appids");
		options.addOption(
				"step",
				true,
				"From which step to run the Engine, 0 For Base ETL; 1 For Cumulative ETL; 2 For Custom Conception ETL; 3 For Report ETL");
		options.addOption("reports", true, "comma-seperated reports titles");
		options.addOption("sqlengine", true,
				"engine name : [both,presto,onlyhive,prestohive,allhive]");
		options.addOption("foldersize", true, "user set folder size");
		options.addOption("stepend", true, "step for end");

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		if (!cmd.hasOption("project")) {
			throw new RuntimeException("project is required!");
		}
		project = cmd.getOptionValue("project");

		if (!cmd.hasOption("ds")) {
			throw new RuntimeException("ds is required!");
		}
		ds = cmd.getOptionValue("ds");

		if (cmd.hasOption("appids")) {
			appids = cmd.getOptionValue("appids");
		}

		if (cmd.hasOption("step")) {
			step = cmd.getOptionValue("step");
		}

		if (cmd.hasOption("reports")) {
			reports = cmd.getOptionValue("reports");
		}

		if (cmd.hasOption("sqlengine")) {
			sqlengine = cmd.getOptionValue("sqlengine");
		}

		if (cmd.hasOption("stepend")) {
			stepend = cmd.getOptionValue("stepend");
		}

		if (cmd.hasOption("foldersize")) {
			foldersize = Integer.parseInt(cmd.getOptionValue("foldersize"));
		}

		enginethreads = Integer.parseInt(cmd.getOptionValue("enginethreads",
				"16"));
		hqlthreads = Integer.parseInt(cmd.getOptionValue("hqlthreads", "16"));

		ETLConfigCtl ecc = null;
		if (project.equalsIgnoreCase("game")) {
			ecc = new GameConfigCtl("/opt/newengine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("qikugame")) {
			ecc = new QikuGameConfigCtl("/home/reyun/engine/settings.properties");
			platform = "qikugame";
		} else if (project.equalsIgnoreCase("track")) {
			ecc = new TrackConfigCtl("/opt/trackengine/settings.properties");
			platform = "track";
		} else if (project.equalsIgnoreCase("zhaohuanshi")) {
			ecc = new GameConfigCtl(
					"/opt/zhaohuanshiengine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("wau")) {
			ecc = new GameConfigCtl("/opt/wauengine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("mau")) {
			ecc = new GameConfigCtl("/opt/mauengine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("rerun")) {
			ecc = new GameConfigCtl("/opt/engine_for_rerun/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("h5")) {
			ecc = new GameConfigCtl("/opt/h5engine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("lm")) {
			ecc = new LMGameConfigCtl("/opt/engine/settings.properties");
			platform = "lm";
		} else if (project.equalsIgnoreCase("enr")) {
			ecc = new GameConfigCtl(
					"/opt/engine_general/engine_noreged/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("lt")) {
			ecc = new LTConfigCtl(
					"/opt/engine_general/ltengine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("market")) {
			ecc = new MarketConfigCtl(
					"/opt/engine_general/marketengine/settings.properties");
			platform = "game";
		} else if (project.equalsIgnoreCase("hcr")) {
			ecc = new HCRConfigCtl(
					"/opt/engine_general/huochairenengine/settings.properties");
			platform = "game";
		} else
			System.exit(1);

		if (Integer.parseInt(stepend) < Integer.parseInt(step))
			System.exit(1);
		BigEngine bigEngine = new BigEngine(ds, enginethreads, hqlthreads,
				appids, step, stepend, reports, ecc, sqlengine, foldersize,
				platform);
		bigEngine.pre_start(sqlengine);
		bigEngine.start();
		bigEngine.post_start();
		// 关闭数据流
		SqlWriterUtils.getInstance().close();
	}
}
