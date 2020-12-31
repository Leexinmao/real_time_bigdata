package com.realtime.flink.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 11:59
 **/
public class DataUtils {



    public static String formatGM8(String time){

        String[] split = time.split("T");
        StringBuffer buffer =  new StringBuffer(split[0]).append(" ");
        String[] split1 = split[1].split(":");
        int hour = Integer.parseInt(split1[0]) + 8;

        buffer.append(hour).append(":")
                .append(split1[1]).append(":").append(split1[2]);
        return buffer.toString();
    }


    public static String getDays(Long times){

        SimpleDateFormat sdf = new SimpleDateFormat(DateStyle.YYYY_MM_DD_HH_MM_SS.getValue());
        Date date = new Date(times);

        return sdf.format(date);



    }





}
