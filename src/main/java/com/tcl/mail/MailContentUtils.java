package com.tcl.mail;

import etlengine.BigEngine;

public class MailContentUtils {
	public static void addMailContent(Exception e, String appid, String sql,
			int retrytime, String title) {
		System.out.println(e.toString()
				+ "------------> the failure appid is: " + appid
				+ ", and the sql is: " + sql);
		// 通过邮件发送此信息给相关人员
		BigEngine.addMailContent("appid is: " + appid.toString()
				+ ", and retrytime is: " + retrytime + " and title is: "
				+ title.toString() + ", and the sql is: " + sql, e.toString());
	}
}
