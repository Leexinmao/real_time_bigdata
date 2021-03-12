package com.realtime.flink.hotitems;

import com.realtime.flink.model.UserBehavior;
import com.realtime.flink.uv.FlinkUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

import javax.annotation.Nullable;

public class HotLetmsSecond {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();


        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(60000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.readTextFile("D:\\bigdate\\gitproject\\real_time_bigdata\\reatime-project\\realtime-flink-data\\src\\main\\resources\\UserBehavior.csv");

        DataStream<UserBehavior> map = stream.map(line -> {
            System.out.println();
            String[] str = line.split(",");
            // Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp
            return new UserBehavior(Long.parseLong(str[0]), Long.parseLong(str[1]), Integer.parseInt(str[2]), str[3], Long.parseLong(str[4]) * 1000L);
        }).filter(r -> r.behavior.equals("pv"));


//        map.assignTimestampsAndWatermarks (new MyAssignTimeStampsAndWatermarks())
//                .keyBy(r -> r.itemId)
//                .timeWindow(Time.minutes(60), Time.minutes(5));




    }


//    class MyAssignTimeStampsAndWatermarks extends AssignerWithPeriodicWatermarksAdapter<UserBehavior>{
//
//    }

}
