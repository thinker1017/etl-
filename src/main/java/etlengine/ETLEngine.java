package etlengine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tcl.conf.ETLConfigCtl;
import com.tcl.util.DateUtils;

public class ETLEngine implements Runnable { // note: one game use one ETLEngine
												// object
	private String appid;
	private String ds;
	private Integer hqlthreads = 16;
	private String sqlengine;
	private ETLConfigCtl ecc;
	private List<ArrayList<String>> baseETLConfs = null; // 0 Step
	private List<ArrayList<String>> cumulativesETLConfs = null; // 1 Step
	private List<ArrayList<String>> conceptionETLConfs = null; // 2 Step
	private List<ArrayList<String>> reportETLConfs = null; // 3 Step
	private List<ArrayList<String>> CLCETLConfs = null; // 4 step
	private String step = null;
	private String stepend = null;
	private String platform = null;

	public ETLEngine(String appid, String ds, String step, ETLConfigCtl ecc,
			String sqlengine, String stepend, String platform)
			throws IOException {
		this.appid = appid;
		this.ds = ds;
		this.step = step;
		this.ecc = ecc;
		this.sqlengine = sqlengine;
		this.stepend = stepend;
		this.platform = platform;
	}

	public void runForBaseETL(Map<String, String> ENV) {
		try {
			ExecutorService exec = Executors.newFixedThreadPool(hqlthreads);
			// ExecutorService exec = Executors.newCachedThreadPool();
			for (ArrayList<String> TEMPLATE_CONF : this.baseETLConfs) {
				HQLThread hqlThread = new HQLThread(ENV, this.appid, this.ecc,
						this.sqlengine, this.platform, this.ds);
				// hqlThread.setHiveconnection(Utils.getHiveConnection()); //
				// 转变为在执行具体线程的时候再初始化hive连接
				hqlThread.setTEMPLATE_ETL_CONF(TEMPLATE_CONF);
				exec.execute(hqlThread);
			}
			exec.shutdown();
			try {
				exec.awaitTermination(12, TimeUnit.HOURS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void runForCumulativesETL(Map<String, String> ENV) {
		try {
			ExecutorService exec = Executors.newFixedThreadPool(hqlthreads);
			for (ArrayList<String> cumulativesETLConf : this.cumulativesETLConfs) {
				HQLThread hqlThread = new HQLThread(ENV, this.appid, this.ecc,
						this.sqlengine, this.platform, this.ds);
				// hqlThread.setHiveconnection(Utils.getHiveConnection());
				hqlThread.setCumulativesETL_CONF(cumulativesETLConf);
				exec.execute(hqlThread);
			}
			exec.shutdown();
			try {
				exec.awaitTermination(12, TimeUnit.HOURS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void runForConceptionETL(Map<String, String> ENV) {
		try {
			ExecutorService exec = Executors.newFixedThreadPool(hqlthreads);
			for (ArrayList<String> conceptionETLConf : this.conceptionETLConfs) {
				HQLThread hqlThread = new HQLThread(ENV, this.appid, this.ecc,
						this.sqlengine, this.platform, this.ds);
				// hqlThread.setHiveconnection(Utils.getHiveConnection());
				hqlThread.setConceptionETLConf(conceptionETLConf);
				exec.execute(hqlThread);
			}
			exec.shutdown();
			try {
				exec.awaitTermination(12, TimeUnit.HOURS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void runForReportETL(Map<String, String> ENV) {
		ExecutorService exec = Executors.newFixedThreadPool(hqlthreads);
		for (ArrayList<String> TEMPLATE_INSERT_CONF : this.reportETLConfs) {
			HQLThread hqlThread = new HQLThread(ENV, this.appid, this.ecc,
					this.sqlengine, this.platform, this.ds);
			try {
				// hqlThread.setHiveconnection(Utils.getHiveConnection());
			} catch (Exception e) {
				e.printStackTrace();
			} // 注意，对于HiveServer1，每个线程必须单独启动一个新的Hive连接对象，否则会造成通信问题
			hqlThread.setTEMPLATE_INSERT_CONF(TEMPLATE_INSERT_CONF);
			exec.execute(hqlThread);
		}
		exec.shutdown();
		try {
			exec.awaitTermination(12, TimeUnit.HOURS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	public void runForCustomLogicChain(Map<String, String> ENV) {
		try {
			ExecutorService exec = Executors.newFixedThreadPool(hqlthreads);
			for (ArrayList<String> clcConf : this.CLCETLConfs) {
				HQLThread hqlThread = new HQLThread(ENV, this.appid, this.ecc,
						this.sqlengine, this.platform, this.ds);
				hqlThread.setCLCETLConf(clcConf);
				exec.execute(hqlThread);
			}
			exec.shutdown();
			try {
				exec.awaitTermination(12, TimeUnit.HOURS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		Map<String, String> ENV = new HashMap<String, String>();
		ENV.put("ds", ds);
		ENV.put("appid", appid);
		ENV.put("year", DateUtils.getYear(ds));
		ENV.put("yearrefertoweek", DateUtils.getYearReferToWeek(ds));
		ENV.put("month", DateUtils.getMonthOfYear(ds));
		ENV.put("week", DateUtils.getWeekOfYear(ds));
		ENV.put("week_start", DateUtils.getWeekStartDate(ds));
		ENV.put("week_end", DateUtils.getWeekEndDate(ds));
		ENV.put("month_start", DateUtils.getMonthStartDate(ds));
		ENV.put("month_end", DateUtils.getMonthEndDate(ds));

		if ("track".equalsIgnoreCase(this.platform)) {
			Map<String, String> trackinfos = new HashMap<String, String>();
			try {
				trackinfos = Utils.getTrackSystemSettings(this.appid, this.ds);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (trackinfos != null && !trackinfos.isEmpty()) {
				ENV.put("installsubdays", trackinfos.get("installsubdays"));
				ENV.put("clicksubdays", trackinfos.get("clicksubdays"));
				ENV.put("normalinstallnumbers",
						trackinfos.get("normalinstallnumbers"));
				ENV.put("normalclicknumbers",
						trackinfos.get("normalclicknumbers"));
				ENV.put("cilag", trackinfos.get("cilag"));
				ENV.put("ciday", trackinfos.get("ciday"));
			} else {
				ENV.put("installsubdays", "30");
				ENV.put("clicksubdays", "30");
				ENV.put("normalinstallnumbers", "30");
				ENV.put("normalclicknumbers", "30");
				ENV.put("cilag", "5");
				ENV.put("ciday", "7");
			}
		} else {

		}

		// 以下三个调用：调用内并行，调用间串行
		this.runHqlTreadByStep(Integer.parseInt(this.step),
				Integer.parseInt(this.stepend), ENV);

	}

	public void runHqlTreadByStep(Integer stepstart, Integer stepend,
			Map<String, String> ENV) {
		Long startTime = System.currentTimeMillis();

		Long startTimeStep = 0l;
		Long endTimeStep = 0l;
		Long deltaTimeStep = 0l;

		for (int i = stepstart; i <= stepend; i++) {
			startTimeStep = System.currentTimeMillis();
			if (i == 0) {
				System.out.println("0 step!");
				this.runForBaseETL(ENV);
			}
			if (i == 1) {
				System.out.println("1st step!");
				this.runForCumulativesETL(ENV);
			}
			if (i == 2) {
				System.out.println("2nd step!");
				this.runForConceptionETL(ENV);
			}
			if (i == 3) {
				System.out.println("3rd step!");
				this.runForCustomLogicChain(ENV);
			}
			if (i == 4) {
				System.out.println("4th step!");
				this.runForReportETL(ENV);
			}

			endTimeStep = System.currentTimeMillis();
			deltaTimeStep = (endTimeStep - startTimeStep) / 1000;
			System.out.println("ReyunTeamETL, appid: " + this.appid + ", Step"
					+ i + " finished in: " + deltaTimeStep + " Seconds!");
		}

		Long endTime = System.currentTimeMillis();
		Long deltaTime = (endTime - startTime) / 1000;
		System.out.println("XXXXXXXX ------> appid: " + this.appid
				+ ", excuted time is: " + deltaTime + " seconds!");
	}

	// Following: getters and setters
	public Integer getHqlthreads() {
		return hqlthreads;
	}

	public void setHqlthreads(Integer hqlthreads) {
		this.hqlthreads = hqlthreads;
	}

	public List<ArrayList<String>> getBaseETLConfs() {
		return baseETLConfs;
	}

	public void setBaseETLConfs(List<ArrayList<String>> baseETLConfs) {
		this.baseETLConfs = baseETLConfs;
	}

	public List<ArrayList<String>> getCumulativesETLConfs() {
		return cumulativesETLConfs;
	}

	public void setCumulativesETLConfs(
			List<ArrayList<String>> cumulativesETLConfs) {
		this.cumulativesETLConfs = cumulativesETLConfs;
	}

	public List<ArrayList<String>> getReportETLConfs() {
		return reportETLConfs;
	}

	public void setReportETLConfs(List<ArrayList<String>> reportETLConfs) {
		this.reportETLConfs = reportETLConfs;
	}

	public List<ArrayList<String>> getConceptionETLConfs() {
		return conceptionETLConfs;
	}

	public void setConceptionETLConfs(List<ArrayList<String>> conceptionETLConfs) {
		this.conceptionETLConfs = conceptionETLConfs;
	}

	public List<ArrayList<String>> getCLCETLConfs() {
		return CLCETLConfs;
	}

	public void setCLCETLConfs(List<ArrayList<String>> cLCETLConfs) {
		CLCETLConfs = cLCETLConfs;
	}
}
