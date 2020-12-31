package com.realtime.flink.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @description: 连接两条流   通过id join 时间范围的流
 * @author: lxm
 * @create: 2020-11-24 00:30
 **/
public class IntervalJoinWindow {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, String, Long>> stream1 = env.fromElements(Tuple3.of("1", "click", 3700 * 1000L),Tuple3.of("2", "click", 3700 * 1000L));

        DataStream<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("1", "browse", 3000 * 1000L),
                Tuple3.of("1", "browse", 3200 * 1000L),
                Tuple3.of("1", "browse", 3400 * 1000L),
                Tuple3.of("1", "browse", 3700 * 1000L),
                Tuple3.of("2", "browse", 3000 * 1000L),
                Tuple3.of("2", "browse", 3200 * 1000L),
                Tuple3.of("2", "browse", 3400 * 1000L),
                Tuple3.of("2", "browse", 3700 * 1000L)
        );

        KeyedStream<Tuple3<String, String, Long>, String> clickStream = stream1.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })).keyBy(r -> r.f0);

        KeyedStream<Tuple3<String, String, Long>, String> browseStream = stream2.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                })).keyBy(r -> r.f0);

        clickStream.intervalJoin(browseStream)
                // 连接之前的600s 到现在
                .between(Time.seconds(-700),Time.seconds(100))
                .process(new MyIntervalJoinFunction())
                .print();

        env.execute("IntervalJoinWindow");


    }

    public static class MyIntervalJoinFunction extends ProcessJoinFunction<Tuple3<String, String, Long>,Tuple3<String, String, Long>,String >{

        @Override
        public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
            out.collect(left +"  >>>>>  left join >>>>>  " + right );
        }
    }


}
