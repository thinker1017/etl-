package com.tcl.util;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

public class RenderUtils {

	public static String render(String template, Map<String, String> context)
			throws ParseErrorException, MethodInvocationException,
			ResourceNotFoundException, IOException {
		StringWriter sw = new StringWriter();
		Context cnxt = new VelocityContext();

		for (Entry<String, String> entry : context.entrySet()) {
			cnxt.put(entry.getKey(), entry.getValue());
		}
		Velocity.evaluate(cnxt, sw, "velocity", template);
		return post_render(sw.toString());
	}

	private static String post_render(String s) {
		String pattern = "(\\d{4}-\\d{2}-\\d{2})([+-]+)(\\d+)";
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(s);
		Calendar cl = Calendar.getInstance();
		while (m.find()) {
			if (m.groupCount() == 3) {
				String ds = m.group(1);
				String sign = m.group(2);
				String delta = m.group(3);
				cl.clear();
				String[] dss = ds.split("-");
				int year = Integer.parseInt(dss[0]);
				int month = Integer.parseInt(dss[1]);
				int day = Integer.parseInt(dss[2]);
				int delta_days = Integer.parseInt(delta);

				cl.set(year, month - 1, day);
				if (sign.equals("+")) {
					cl.add(Calendar.DAY_OF_YEAR, delta_days);
				} else {
					cl.add(Calendar.DAY_OF_YEAR, -delta_days);
				}
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				String the_date = sdf.format(cl.getTime());
				s = s.replace(m.group(0), the_date);
			}
		}
		return s;
	}

	public static String[] split(String t, String sep) {
		return t.split(sep);
	}

}
