/**
 * @Title: DateUtil.java
 * @Package com.xingyun.xyb2b.common.util
 * @Description: TODO(用一句话描述该文件做什么)
 * @author bond
 * @date 2016年8月30日 下午4:32:30
 * @company 版权所有 深圳市天行云供应链有限公司
 * @version V1.0
 */
package com.realtime.flink.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @ClassName: DateUtil
 * @Description: 日期工具类
 * @author bond
 * @date 2016年8月30日 下午4:32:30
 *
 */
public class DateUtil {
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");


	/**
	 * 获取SimpleDateFormat
	 *
	 * @param parttern
	 *            日期格式
	 * @return SimpleDateFormat对象
	 * @throws RuntimeException
	 *             异常：非法日期格式
	 */
	public static SimpleDateFormat getDateFormat(String parttern) throws RuntimeException {
		return new SimpleDateFormat(parttern);
	}

	/**
	 * 获取日期中的某数值。如获取月份
	 *
	 * @param date
	 *            日期
	 * @param dateType
	 *            日期格式
	 * @return 数值
	 */
	private static int getInteger(Date date, int dateType) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(dateType);
	}

	/**
	 * 增加日期中某类型的某数值。如增加日期
	 *
	 * @param date
	 *            日期字符串
	 * @param dateType
	 *            类型
	 * @param amount
	 *            数值
	 * @return 计算后日期字符串
	 */
	private static String addInteger(String date, int dateType, int amount) {
		String dateString = null;
		DateStyle dateStyle = getDateStyle(date);
		if (dateStyle != null) {
			Date myDate = StringToDate(date, dateStyle);
			myDate = addInteger(myDate, dateType, amount);
			dateString = DateToString(myDate, dateStyle);
		}
		return dateString;
	}

	/**
	 * 增加日期中某类型的某数值。如增加日期
	 *
	 * @param date
	 *            日期
	 * @param dateType
	 *            类型
	 * @param amount
	 *            数值
	 * @return 计算后日期
	 */
	private static Date addInteger(Date date, int dateType, int amount) {
		Date myDate = null;
		if (date != null) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(dateType, amount);
			myDate = calendar.getTime();
		}
		return myDate;
	}

	/**
	 * 获取精确的日期
	 *
	 * @param timestamps
	 *            时间long集合
	 * @return 日期
	 */
	private static Date getAccurateDate(List<Long> timestamps) {
		Date date = null;
		long timestamp = 0;
		Map<Long, long[]> map = new HashMap<Long, long[]>();
		List<Long> absoluteValues = new ArrayList<Long>();

		if (timestamps != null && timestamps.size() > 0) {
			if (timestamps.size() > 1) {
				for (int i = 0; i < timestamps.size(); i++) {
					for (int j = i + 1; j < timestamps.size(); j++) {
						long absoluteValue = Math.abs(timestamps.get(i) - timestamps.get(j));
						absoluteValues.add(absoluteValue);
						long[] timestampTmp = { timestamps.get(i), timestamps.get(j) };
						map.put(absoluteValue, timestampTmp);
					}
				}

				// 有可能有相等的情况。如2012-11和2012-11-01。时间戳是相等的
				long minAbsoluteValue = -1;
				if (!absoluteValues.isEmpty()) {
					// 如果timestamps的size为2，这是差值只有一个，因此要给默认值
					minAbsoluteValue = absoluteValues.get(0);
				}
				for (int i = 0; i < absoluteValues.size(); i++) {
					for (int j = i + 1; j < absoluteValues.size(); j++) {
						if (absoluteValues.get(i) > absoluteValues.get(j)) {
							minAbsoluteValue = absoluteValues.get(j);
						} else {
							minAbsoluteValue = absoluteValues.get(i);
						}
					}
				}

				if (minAbsoluteValue != -1) {
					long[] timestampsLastTmp = map.get(minAbsoluteValue);
					if (absoluteValues.size() > 1) {
						timestamp = Math.max(timestampsLastTmp[0], timestampsLastTmp[1]);
					} else if (absoluteValues.size() == 1) {
						// 当timestamps的size为2，需要与当前时间作为参照
						long dateOne = timestampsLastTmp[0];
						long dateTwo = timestampsLastTmp[1];
						if ((Math.abs(dateOne - dateTwo)) < 100000000000L) {
							timestamp = Math.max(timestampsLastTmp[0], timestampsLastTmp[1]);
						} else {
							long now = new Date().getTime();
							if (Math.abs(dateOne - now) <= Math.abs(dateTwo - now)) {
								timestamp = dateOne;
							} else {
								timestamp = dateTwo;
							}
						}
					}
				}
			} else {
				timestamp = timestamps.get(0);
			}
		}

		if (timestamp != 0) {
			date = new Date(timestamp);
		}
		return date;
	}

	/**
	 * 判断字符串是否为日期字符串
	 *
	 * @param date
	 *            日期字符串
	 * @return true or false
	 */
	public static boolean isDate(String date) {
		boolean isDate = false;
		if (date != null) {
			if (StringToDate(date) != null) {
				isDate = true;
			}
		}
		return isDate;
	}

	/**
	 * 获取日期字符串的日期风格。失敗返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @return 日期风格
	 */
	public static DateStyle getDateStyle(String date) {
		DateStyle dateStyle = null;
		Map<Long, DateStyle> map = new HashMap<Long, DateStyle>();
		List<Long> timestamps = new ArrayList<Long>();
		for (DateStyle style : DateStyle.values()) {
			Date dateTmp = StringToDate(date, style.getValue());
			if (dateTmp != null) {
				timestamps.add(dateTmp.getTime());
				map.put(dateTmp.getTime(), style);
			}
		}
		dateStyle = map.get(getAccurateDate(timestamps).getTime());
		return dateStyle;
	}

	/**
	 * 将日期字符串转化为日期。失败返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @return 日期
	 */
	public static Date StringToDate(String date) {
		DateStyle dateStyle = DateStyle.YYYY_MM_DD_HH_MM_SS;
		return StringToDate(date, dateStyle);
	}

	/**
	 * 将日期字符串转化为日期。失败返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @param parttern
	 *            日期格式
	 * @return 日期
	 */
	public static Date StringToDate(String date, String parttern) {
		Date myDate = null;
		if (date != null) {
			try {
				myDate = getDateFormat(parttern).parse(date);
			} catch (Exception e) {
			}
		}
		return myDate;
	}

	/**
	 * 将日期字符串转化为日期。失败返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @param dateStyle
	 *            日期风格
	 * @return 日期
	 */
	public static Date StringToDate(String date, DateStyle dateStyle) {
		Date myDate = null;
		if (dateStyle == null) {
			List<Long> timestamps = new ArrayList<Long>();
			for (DateStyle style : DateStyle.values()) {
				Date dateTmp = StringToDate(date, style.getValue());
				if (dateTmp != null) {
					timestamps.add(dateTmp.getTime());
				}
			}
			myDate = getAccurateDate(timestamps);
		} else {
			myDate = StringToDate(date, dateStyle.getValue());
		}
		return myDate;
	}

	/**
	 * 将日期转化为日期字符串。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param parttern
	 *            日期格式
	 * @return 日期字符串
	 */
	public static String DateToString(Date date, String parttern) {
		String dateString = null;
		if (date != null) {
			try {
				dateString = getDateFormat(parttern).format(date);
			} catch (Exception e) {
			}
		}
		return dateString;
	}

	/**
	 * 将日期转化为日期字符串。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param dateStyle
	 *            日期风格
	 * @return 日期字符串
	 */
	public static String DateToString(Date date, DateStyle dateStyle) {
		String dateString = null;
		if (dateStyle != null) {
			dateString = DateToString(date, dateStyle.getValue());
		}
		return dateString;
	}

	/**
	 * 将日期字符串转化为另一日期字符串。失败返回null。
	 *
	 * @param date
	 *            旧日期字符串
	 * @param parttern
	 *            新日期格式
	 * @return 新日期字符串
	 */
	public static String StringToString(String date, String parttern) {
		return StringToString(date, null, parttern);
	}

	/**
	 * 将日期字符串转化为另一日期字符串。失败返回null。
	 *
	 * @param date
	 *            旧日期字符串
	 * @param dateStyle
	 *            新日期风格
	 * @return 新日期字符串
	 */
	public static String StringToString(String date, DateStyle dateStyle) {
		return StringToString(date, null, dateStyle);
	}

	/**
	 * 将日期字符串转化为另一日期字符串。失败返回null。
	 *
	 * @param date
	 *            旧日期字符串
	 * @param olddParttern
	 *            旧日期格式
	 * @param newParttern
	 *            新日期格式
	 * @return 新日期字符串
	 */
	public static String StringToString(String date, String olddParttern, String newParttern) {
		String dateString = null;
		if (olddParttern == null) {
			DateStyle style = getDateStyle(date);
			if (style != null) {
				Date myDate = StringToDate(date, style.getValue());
				dateString = DateToString(myDate, newParttern);
			}
		} else {
			Date myDate = StringToDate(date, olddParttern);
			dateString = DateToString(myDate, newParttern);
		}
		return dateString;
	}

	/**
	 * 将日期字符串转化为另一日期字符串。失败返回null。
	 *
	 * @param date
	 *            旧日期字符串
	 * @param olddDteStyle
	 *            旧日期风格
	 * @param newDateStyle
	 *            新日期风格
	 * @return 新日期字符串
	 */
	public static String StringToString(String date, DateStyle olddDteStyle, DateStyle newDateStyle) {
		String dateString = null;
		if (olddDteStyle == null) {
			DateStyle style = getDateStyle(date);
			dateString = StringToString(date, style.getValue(), newDateStyle.getValue());
		} else {
			dateString = StringToString(date, olddDteStyle.getValue(), newDateStyle.getValue());
		}
		return dateString;
	}

	/**
	 * 增加日期的年份。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param yearAmount
	 *            增加数量。可为负数
	 * @return 增加年份后的日期字符串
	 */
	public static String addYear(String date, int yearAmount) {
		return addInteger(date, Calendar.YEAR, yearAmount);
	}

	/**
	 * 增加日期的年份。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param yearAmount
	 *            增加数量。可为负数
	 * @return 增加年份后的日期
	 */
	public static Date addYear(Date date, int yearAmount) {
		return addInteger(date, Calendar.YEAR, yearAmount);
	}

	/**
	 * 增加日期的月份。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param yearAmount
	 *            增加数量。可为负数
	 * @return 增加月份后的日期字符串
	 */
	public static String addMonth(String date, int yearAmount) {
		return addInteger(date, Calendar.MONTH, yearAmount);
	}

	/**
	 * 增加日期的月份。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param yearAmount
	 *            增加数量。可为负数
	 * @return 增加月份后的日期
	 */
	public static Date addMonth(Date date, int yearAmount) {
		return addInteger(date, Calendar.MONTH, yearAmount);
	}

	/**
	 * 增加日期的天数。失败返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @param dayAmount
	 *            增加数量。可为负数
	 * @return 增加天数后的日期字符串
	 */
	public static String addDay(String date, int dayAmount) {
		return addInteger(date, Calendar.DATE, dayAmount);
	}

	/**
	 * 增加日期的天数。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @param dayAmount
	 *            增加数量。可为负数
	 * @return 增加天数后的日期
	 */
	public static Date addDay(Date date, int dayAmount) {
		return addInteger(date, Calendar.DATE, dayAmount);
	}

	/**
	 * 增加日期的小时。失败返回null。
	 *
	 * @param date
	 *            日期字符串

	 *            增加数量。可为负数
	 * @return 增加小时后的日期字符串
	 */
	public static String addHour(String date, int hourAmount) {
		return addInteger(date, Calendar.HOUR_OF_DAY, hourAmount);
	}

	/**
	 * 增加日期的小时。失败返回null。
	 *
	 * @param date
	 *            日期

	 *            增加数量。可为负数
	 * @return 增加小时后的日期
	 */
	public static Date addHour(Date date, int hourAmount) {
		return addInteger(date, Calendar.HOUR_OF_DAY, hourAmount);
	}


	public static String addMinute(String date, int hourAmount) {
		return addInteger(date, Calendar.MINUTE, hourAmount);
	}

	/**
	 * 增加日期的分钟。失败返回null。
	 *
	 * @param date
	 *            日期

	 *            增加数量。可为负数
	 * @return 增加分钟后的日期
	 */
	public static Date addMinute(Date date, int hourAmount) {
		return addInteger(date, Calendar.MINUTE, hourAmount);
	}

	/**
	 * 增加日期的秒钟。失败返回null。
	 *
	 * @param date
	 *            日期字符串

	 *            增加数量。可为负数
	 * @return 增加秒钟后的日期字符串
	 */
	public static String addSecond(String date, int hourAmount) {
		return addInteger(date, Calendar.SECOND, hourAmount);
	}

	/**
	 * 增加日期的秒钟。失败返回null。
	 *
	 * @param date
	 *            日期

	 *            增加数量。可为负数
	 * @return 增加秒钟后的日期
	 */
	public static Date addSecond(Date date, int hourAmount) {
		return addInteger(date, Calendar.SECOND, hourAmount);
	}

	/**
	 * 获取日期的年份。失败返回0。
	 *
	 * @param date
	 *            日期字符串
	 * @return 年份
	 */
	public static int getYear(String date) {
		return getYear(StringToDate(date));
	}

	/**
	 * 获取日期的年份。失败返回0。
	 *
	 * @param date
	 *            日期
	 *
	 * @return 年份
	 */
	public static int getYear(Date date) {
		return getInteger(date, Calendar.YEAR);
	}

	/**
	 * 获取日期的月份。失败返回0。
	 *
	 * @param date
	 *            日期字符串
	 * @return 月份
	 */
	public static int getMonth(String date) {
		return getMonth(StringToDate(date));
	}

	/**
	 * 获取日期的月份。失败返回0。
	 *
	 * @param date
	 *            日期
	 * @return 月份
	 */
	public static int getMonth(Date date) {
		return getInteger(date, Calendar.MONTH);
	}

	/**
	 * 获取日期的天数。失败返回0。
	 *
	 * @param date
	 *            日期字符串
	 * @return 天
	 */
	public static int getDay(String date) {
		return getDay(StringToDate(date));
	}

	/**
	 * 获取日期的天数。失败返回0。
	 *
	 * @param date
	 *            日期
	 * @return 天
	 */
	public static int getDay(Date date) {
		return getInteger(date, Calendar.DATE);
	}

	/**
	 * 获取日期的小时。失败返回0。
	 *
	 * @param date
	 *            日期字符串
	 * @return 小时
	 */
	public static int getHour(String date) {
		return getHour(StringToDate(date));
	}

	/**
	 * 获取日期的小时。失败返回0。
	 *
	 * @param date
	 *            日期
	 * @return 小时
	 */
	public static int getHour(Date date) {
		return getInteger(date, Calendar.HOUR_OF_DAY);
	}

	/**
	 * 获取日期的分钟。失败返回0。
	 *
	 * @param date
	 *            日期字符串
	 * @return 分钟
	 */
	public static int getMinute(String date) {
		return getMinute(StringToDate(date));
	}

	/**
	 * 获取日期的分钟。失败返回0。
	 *
	 * @param date
	 *            日期
	 * @return 分钟
	 */
	public static int getMinute(Date date) {
		return getInteger(date, Calendar.MINUTE);
	}

	/**
	 * 获取日期的秒钟。失败返回0。
	 *
	 * @param date
	 *            日期字符串
	 * @return 秒钟
	 */
	public static int getSecond(String date) {
		return getSecond(StringToDate(date));
	}

	/**
	 * 获取日期的秒钟。失败返回0。
	 *
	 * @param date
	 *            日期
	 * @return 秒钟
	 */
	public static int getSecond(Date date) {
		return getInteger(date, Calendar.SECOND);
	}

	/**
	 * 获取日期 。默认yyyy-MM-dd格式。失败返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @return 日期
	 */
	public static String getDate(String date) {
		return StringToString(date, DateStyle.YYYY_MM_DD);
	}

	/**
	 * 获取日期。默认yyyy-MM-dd格式。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @return 日期
	 */
	public static String getDate(Date date) {
		return DateToString(date, DateStyle.YYYY_MM_DD);
	}

	/**
	 * 获取日期。默认yyyy-MM-dd HH:mm:ss格式。失败返回null。
	 * @param date
	 * @return
	 */
	public static String getDateTime(Date date) {
		return DateToString(date, DateStyle.YYYY_MM_DD_HH_MM_SS);
	}

	/**
	 * 获取日期的时间。默认HH:mm:ss格式。失败返回null。
	 *
	 * @param date
	 *            日期字符串
	 * @return 时间
	 */
	public static String getTime(String date) {
		return StringToString(date, DateStyle.HH_MM_SS);
	}

	/**
	 * 获取日期的时间。默认HH:mm:ss格式。失败返回null。
	 *
	 * @param date
	 *            日期
	 * @return 时间
	 */
	public static String getTime(Date date) {
		return DateToString(date, DateStyle.HH_MM_SS);
	}


	/**
	 * 获取两个日期相差的天数
	 *
	 * @param date
	 *            日期字符串
	 * @param otherDate
	 *            另一个日期字符串
	 * @return 相差天数
	 */
	public static int getIntervalDays(String date, String otherDate) {
		return getIntervalDays(StringToDate(date), StringToDate(otherDate));
	}

	/**
	 * @param date
	 *            日期
	 * @param otherDate
	 *            另一个日期
	 * @return 相差天数
	 */
	public static int getIntervalDays(Date date, Date otherDate) {
		date = DateUtil.StringToDate(DateUtil.getDate(date));
		long time = Math.abs(date.getTime() - otherDate.getTime());
		return (int) (time / (24 * 60 * 60 * 1000));
	}

	/**
	 * 判断闰年
	 *
	 * @param year
	 *            年
	 * @return bool
	 */
	public static boolean check(int year) {
		if ((year % 4 == 0) && ((year % 100 != 0) | (year % 400 == 0))) {
			return true;
		} else {
			return false;
		}
	}
	public static int dayInCurrentMonth(int month,int year){
		int day = 0;
		if (DateUtil.check(year)) {
			if (month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) {
				day = 31;
			} else if (month == 2) {
				day = 29;
			} else {
				day = 30;
			}
		} else {
			if (month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) {
				day = 31;
			} else if (month == 2) {
				day = 28;
			} else {
				day = 30;
			}
		}
		return day;
	}
	public static String addSubtractTime(int number, int type) {
		Date d = new Date();
		String dateTime="";
		if (type == 1) {
			dateTime=df.format(new Date(d.getTime() + (long) number * 24 * 60 * 60 * 1000));
		} else if (type == 2) {
			dateTime=df.format(new Date(d.getTime() - (long) number * 24 * 60 * 60 * 1000));
		}

		return dateTime;

	}

	/**
	 * @Title: getLastActualDayOfMonth
	 * @Description: 取得该月最后一个自然日
	 * @param @param year
	 * @param @param month
	 * @param @return  参数说明
	 * @return Date    返回类型
	 * @author leo
	 * @throws
	 */
	public static Date getLastActualDayOfMonth(int year, int month) {
		if (!isActiveYear(year)||!isActiveMonth(month)) {
			return null;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month - 1);
		int lastDay = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
		calendar.set(Calendar.DAY_OF_MONTH, lastDay);
		return calendar.getTime();
	}

	/**
	 * @Title: isLastActualDayOfMonth
	 * @Description: 判断该日期是否为该月最后一天(自然日)
	 * @param @param date
	 * @param @return  参数说明
	 * @return boolean    返回类型
	 * @author leo
	 * @throws
	 */
	public static boolean isLastActualDayOfMonth(Date date) {
		if (date!=null) {
			return false;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		Date lastActualDayOfMonth = getLastActualDayOfMonth(
				calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1);
		return df.format(date).equals(df.format(lastActualDayOfMonth));
	}

	/**
	 * @Title: isActiveYear
	 * @Description: 判断年份是否是有效的 大于1900且小于9999
	 * @param @param year
	 * @param @return  参数说明
	 * @return boolean    返回类型
	 * @author leo
	 * @throws
	 */
	private static boolean isActiveYear(int year) {
		return year > 1900 && year < 9999;
	}

	/**
	 * @Title: isActiveMonth
	 * @Description: 判断月份是否有效
	 * @param @param month
	 * @param @return  参数说明
	 * @return boolean    返回类型
	 * @author leo
	 * @throws
	 */
	private static boolean isActiveMonth(int month) {
		return month > 0 && month < 13;
	}

	public static String getCurrentFirstDayByYear(){
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_YEAR, 1);
		return df.format(calendar.getTime());
	}
	public static String getCurrentFirstDayByWeek(){
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_WEEK, 2);
		return df.format(calendar.getTime());
	}
	//	public static String getCurrentFirstDayByDay(){
//
//	}
	public static String getCurrentFirstDayByMonth(){
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		return df.format(calendar.getTime());
	}

	/**
	 * @Title: getaaa
	 * @Description: 按每周的第一天为星期一算，今天是第几周
	 * @return
	 * @author Tito
	 */
	public static int getWeekIndexByMondayIsFirstWeekday() {
		boolean firstDayIsMonday = false; // 第一天是否星期一。如果不是，那么算出的周+1
		Calendar firstDate = Calendar.getInstance(); // 一年的第一天
		firstDate.setTime(new Date());
		firstDate.set(Calendar.MONTH, 0); // 1月
		firstDate.set(Calendar.DATE, 1); // 1号
		int dayOfWeek = firstDate.get(Calendar.DAY_OF_WEEK); // 1月1号是星期几
		if (dayOfWeek == 2) {	//是否星期一
			firstDayIsMonday = true;
		}
		int firstMondayDate = (10 - dayOfWeek) % 7; // 第一个星期一是几号
		firstMondayDate = firstMondayDate == 0 ? 7 : firstMondayDate;
		Calendar now = Calendar.getInstance(); // 今天
		now.setTime(new Date());
		int dayOfYear = now.get(Calendar.DAY_OF_YEAR); // 今天是今年第几天
		int nowWeekIndex = (dayOfYear - firstMondayDate) / 7 + 1; // 今天是第几周
		if (!firstDayIsMonday) {
			nowWeekIndex++;
		}
		return nowWeekIndex;
	}

	/**
	 * @Title: getLastMonthSameDateOrEndDate
	 * @Description: 计算上个月的今天或上个月的最后一天。
	 * @param currentDate 日期
	 * @return
	 * @author Tito
	 */
	public static String getLastMonthSameDateOrEndDate(String currentDate){
		Calendar c = Calendar.getInstance();
		c.setTime(DateUtil.StringToDate(currentDate, DateStyle.YYYY_MM_DD));
		int dayOfMonth = c.get(Calendar.DAY_OF_MONTH);	//今天是本月第几天
		c.set(Calendar.DAY_OF_MONTH, 1);	// 本月第一天
		c.set(Calendar.MONTH, c.get(Calendar.MONTH) - 1);	// 上个月第一天
		int lastMonthDay = c.getActualMaximum(Calendar.DATE);	// 上个月总共有多少天
		c.set(Calendar.DAY_OF_MONTH, dayOfMonth > lastMonthDay ? lastMonthDay : dayOfMonth);
		return DateUtil.getDate(c.getTime());
	}

	/**
	 *
	 * @Title: nowIsbetweenDates
	 * @Description: 判断当前时间是否处于某个时间段(天)
	 * @param startDate 开始时间
	 * @param endDate 结束时间
	 * @return
	 * @author duyy
	 */
	public static boolean nowIsbetweenDates(String startDate, String endDate){
		String today = DateToString(new Date(), "yyyy-MM-dd");
		if( startDate.compareTo(today)  <= 0 && endDate.compareTo(today) > 0){
			return true;
		}else{
			return false;
		}
	}

	/**
	 * 比较当前时间是否在区间 精确到秒 yyyy-MM-dd HH:mm:ss
	 * @Title: nowIfbetweenDates
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param startDate
	 * @param @param endDate
	 * @param @return  参数说明
	 * @return boolean    返回类型
	 * @author sup
	 * @throws
	 */
	public static boolean nowIfbetweenDates(String startDate, String endDate){
		String today = DateToString(new Date(), "yyyy-MM-dd HH:mm:ss");
		if( startDate.compareTo(today)  <= 0 && endDate.compareTo(today) > 0){
			return true;
		}else{
			return false;
		}
	}

	/**
	 * 比较传入的时间 是否在区间  精确到秒 yyyy-MM-dd HH:mm:ss
	 * @Title: dateIfbetweenDates
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param date
	 * @param @param startDate
	 * @param @param endDate
	 * @param @return  参数说明
	 * @return boolean    返回类型
	 * @author sup
	 * @throws
	 */
	public static boolean dateIfbetweenDates(String date,String startDate, String endDate){
		//String today = DateToString(new Date(), "yyyy-MM-dd HH:mm:ss");
		if( startDate.compareTo(date)  <= 0 && endDate.compareTo(date) > 0){
			return true;
		}else{
			return false;
		}
	}

	/**
	 * 获取指定格式时间
	 * @Title: formatDateByPattern
	 * @Description: TODO(这里用一句话描述这个方法的作用)
	 * @param @param date
	 * @param @param dateFormat
	 * @param @return  参数说明
	 * @return String    返回类型
	 * @author sup
	 * @throws
	 */
	public static String formatDateByPattern(Date date,String dateFormat){
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		String formatTimeStr = null;
		if (date != null) {
			formatTimeStr = sdf.format(date);
		}
		return formatTimeStr;
	}
	/***
	 * 根据时间转换成时间点cron表达式
	 * convert Date to cron ,eg.  "0 06 10 15 1 ? 2014"
	 * @param date  : 时间点
	 * @return
	 */
	public static String getCron(Date  date){
		String dateFormat="ss mm HH dd MM ? yyyy";
		return formatDateByPattern(date, dateFormat);
	}

	/**
	 * 获取当前日期是星期几<br>
	 *
	 * @param dt
	 * @return 当前日期是星期几
	 */
	public static String getWeekOfDate(Date dt) {
		String[] weekDays = {"星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
		if (w < 0)
			w = 0;
		return weekDays[w];
	}

	public static int getWeekNo(Date dt) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		int w = cal.get(Calendar.DAY_OF_WEEK)-1;
		if (w < 0)
			w = 0;
		return w;
	}

	/**
	 *
	 * @Title: getDateByWeekDay
	 * @Description: TODO(获取按前或后几周 周几的日期)
	 * @param @param n为推迟的周数，1本周，-1向前推迟一周，2下周，依次类推
	 * @param @param weekNo 周日 1 周一2 周六 7依次类推  想周几，这里就传几Calendar.MONDAY（TUESDAY...）
	 * @param @return  参数说明
	 * @return String    返回类型
	 * @author sup
	 * @throws
	 */
	public static String getDateByWeekDay(int n,int weekNo){
		Calendar cal = Calendar.getInstance();
		//n为推迟的周数，1本周，-1向前推迟一周，2下周，依次类推

		String monday;

		cal.add(Calendar.DATE, n*7);

		//想周几，这里就传几Calendar.MONDAY（TUESDAY...）

		cal.set(Calendar.DAY_OF_WEEK,weekNo);

		monday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());

		//System.out.println("====="+monday);
		return monday;
	}

	public static String addSecondNew(String ti, Integer i) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date1;
		try {
			date1 = sdf.parse(ti);
			long date = date1.getTime() + i*1000l;
			return DateToString(new Date(date), DateStyle.YYYY_MM_DD_HH_MM_SS);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			return ti;
		}

	}

	/**
	 * 获取日期字符串的日期风格。失敗返回null。
	 * @param date
	 * @return
	 */
	public static DateStyle getNewDateStyle(String date) {
		for (DateStyle style : DateStyle.values()) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(style.getValue());
			try {
				LocalDateTime localDateTime = LocalDateTime.parse(date, formatter);
				return style;
			}catch (Exception e){
				//转换失败
				continue;
			}
		}
		return null;
	}

	/**
	 * 判断时间字符串格式是否相同
	 * @param dateTime
	 * @param otherTime
	 * @return
	 */
	public static boolean isSameStyle(String dateTime,String otherTime){
		return getNewDateStyle(dateTime).equals(getNewDateStyle(otherTime));
	}

	/**
	 * 获取对应时间格式的当前时间
	 * @param style
	 * @return
	 */
	public static String getDateTime(DateStyle style){
		LocalDateTime dateTime = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(style.getValue());
		return formatter.format(dateTime);
	}

	/**
	 * 获取所传时间得往后hours小时的时间
	 * @param dateTime
	 * @param hours
	 * @return
	 */
	public static String plusHours(String dateTime,int hours){
		DateStyle dateStyle = getNewDateStyle(dateTime);
		if (null == dateStyle){
			return null;
		}
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateStyle.getValue());
		LocalDateTime localDateTime = LocalDateTime.parse(dateTime,formatter).plusHours(hours);
		return formatter.format(localDateTime);
	}

	/**
	 * 获取所传时间得往后minutes分钟的时间
	 * @param dateTime
	 * @param minutes
	 * @return
	 */
	public static String plusMinutes(String dateTime,int minutes){
		DateStyle dateStyle = getNewDateStyle(dateTime);
		if (null == dateStyle){
			return null;
		}
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateStyle.getValue());
		LocalDateTime localDateTime = LocalDateTime.parse(dateTime,formatter).plusMinutes(minutes);
		return formatter.format(localDateTime);
	}

	/**
	 * 判断时间dateTime 是否在otherTime 之后（格式必须相同，否则返回false）
	 * @param dateTime
	 * @param otherTime
	 * @return Boolean
	 */
	public static Boolean isAfter(String dateTime,String otherTime){
		DateStyle dateTimeStyle = getNewDateStyle(dateTime);
		DateStyle otherTimeeStyle = getNewDateStyle(otherTime);
		if (null == dateTimeStyle || null == otherTimeeStyle || dateTimeStyle != otherTimeeStyle){
			return false;
		}
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimeStyle.getValue());
		LocalDateTime localDateTime = LocalDateTime.parse(dateTime,formatter);
		LocalDateTime localOtherDateTime = LocalDateTime.parse(otherTime,formatter);
		return localDateTime.isAfter(localOtherDateTime);
	}

	/**
	 * 获取月份一号
	 *
	 * @param month 上月为-1 下月为+1
	 * @param day 指定的多少号
	 * @return
	 */
	public static String dataformat(int month,int day){

		SimpleDateFormat format = new SimpleDateFormat(DateStyle.YYYY_MM_DD.getValue()); //"yyyy-MM-dd"
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.MONTH, month);
		cal.set(Calendar.DAY_OF_MONTH,day);
		String time = format.format(cal.getTime());
		return time;
	}

	/**
	 *
	 * @param day 传0为今天 传1为昨天
	 * @return
	 */
	public static String getYesterdayStr(int day) {
		return DateUtil.getDate(DateUtil.addDay(new Date(), day));
	}


	/**
	 *
	 * @param date 传的日期
	 * @param style 日期格式 如"yyyy-MM-dd"
	 * @return 返回日期格式字符串
	 */
	public static String formatDateToString(Date date,String style){
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(style);
		return simpleDateFormat.format(date);
	}

	/**
	 * 计算两个日期的时间差（天）
	 * @param startDate 起始时间
	 * @param endDate 结束时间
	 * @return 返回天数
	 */
	public static int differentDays(Date startDate, Date endDate){
		Long  days = (endDate.getTime() - startDate.getTime())/(60*60*24*1000);
		return days.intValue();
	}

	/**
	 *
	 * @param month 某年某月
	 * @return 返回该月有多少天
	 */
	public static int getMonthOfDays(String month)  {
		Date date = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(DateStyle.YYYY_MM.getValue());//"yyyy-MM"
			date = sdf.parse(month);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
	}

}
