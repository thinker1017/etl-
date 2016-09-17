package com.tcl.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

	public static Calendar getCalendarFromString(String ds) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = sdf.parse(ds);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar;
	}

	public static String getWeekStartDate(String ds) {
		Calendar calendar = getCalendarFromString(ds);
		Date firstDateOfWeek;
		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;
		if (dayOfWeek == 0) {
			dayOfWeek = 7;
		}
		calendar.add(Calendar.DAY_OF_WEEK, 1 - (dayOfWeek));
		firstDateOfWeek = calendar.getTime();
		// calendar.add(Calendar.DAY_OF_WEEK, 6);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(firstDateOfWeek);
	}

	public static String getWeekEndDate(String ds) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = getCalendarFromString(ds);
		Date lastDateOfWeek;
		int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;
		if (dayOfWeek == 0) {
			dayOfWeek = 7;
		}
		calendar.add(Calendar.DAY_OF_WEEK, 7 - (dayOfWeek));
		// calendar.add(Calendar.DAY_OF_WEEK, 6);
		lastDateOfWeek = calendar.getTime();
		return sdf.format(lastDateOfWeek);
	}

	public static String getWeekOfYear(String ds) {
		Calendar calendar = getCalendarFromString(ds);
		return Integer.toString(calendar.get(Calendar.WEEK_OF_YEAR));
	}

	public static String getMonthOfYear(String ds) {
		Calendar calendar = getCalendarFromString(ds);
		return Integer.toString(calendar.get(Calendar.MONTH) + 1);
	}

	public static String getYear(String ds) {
		Calendar calendar = getCalendarFromString(ds);
		return Integer.toString(calendar.get(Calendar.YEAR));
	}

	public static String getMonthStartDate(String ds) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = getCalendarFromString(ds);
		calendar.set(Calendar.DAY_OF_MONTH,
				calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
		return sdf.format(calendar.getTime());
	}

	public static String getMonthEndDate(String ds) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = getCalendarFromString(ds);
		calendar.set(Calendar.DAY_OF_MONTH,
				calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		return sdf.format(calendar.getTime());
	}

	public static String getYearReferToWeek(String ds) {
		Calendar calendar = getCalendarFromString(ds);
		int todayWeek = calendar.get(Calendar.WEEK_OF_YEAR);
		int todayYear = calendar.get(Calendar.YEAR);
		calendar.add(Calendar.DAY_OF_YEAR, -7);
		int sevenDaysBeforeWeek = calendar.get(Calendar.WEEK_OF_YEAR);
		int sevenDaysBeforeYear = calendar.get(Calendar.YEAR);
		if (sevenDaysBeforeWeek == 52) {
			if (todayWeek == 1) {
				if (todayYear == sevenDaysBeforeYear) {
					calendar.add(Calendar.YEAR, 1);
				}
			}
		}
		calendar.add(Calendar.DAY_OF_YEAR, 7); // 还原
		return Integer.toString(calendar.get(Calendar.YEAR));
	}

	public static String getSystemDate() {
		Date today = new Date();
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		return f.format(today);
	}
}
