package com.realtime.hot.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-29 17:41
 **/
public class DateTimeUtils {


    public static String dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        String res = String.valueOf(ts);
        return res;
    }


    public static String stampToDataTime(Long timestamp) throws ParseException {

        Date date = new Date(timestamp);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(date);

    }

}
