package com.realtime.flink.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-24 22:17
 **/
public class WindowJoinFunction {


    private static OutputTag<String> output =  new OutputTag<String>("se"){};

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, Integer, Long>> input1 = env.fromElements(
                Tuple3.of("a", 1, 2000L),
                Tuple3.of("b", 1, 3000L),
                Tuple3.of("c", 1, 4000L),
                Tuple3.of("d", 1, 5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, Integer, Long> element, long recordTimestamp) {
                return element.f2;
            }
        }));;

        DataStream<Tuple3<String, String, Long>> input2 = env.fromElements(
                Tuple3.of("ef", "a", 2000L),
                Tuple3.of("ff", "a", 2000L),
                Tuple3.of("gf", "a", 2000L),
                Tuple3.of("dh", "b", 3000L),
                Tuple3.of("dh", "b", 3000L),
                Tuple3.of("dh", "d", 5000L),
                Tuple3.of("dh", "d", 5000L),
                Tuple3.of("dh", "c", 4000L),
                Tuple3.of("dh", "c", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                return element.f2;
            }
        }));

        input1.join(input2)
                .where(r->r.f0)
                .equalTo(t->t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10s的窗口
                .apply(new MyJoinFunction())
                .print();

        env.execute("WindowJoinFunctionss");

    }

    public static class MyJoinFunction implements JoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, String, Long>, String> {

        @Override
        public String join(Tuple3<String, Integer, Long> first, Tuple3<String, String, Long> second) throws Exception {
            return "结果为  ===  " + first  + "   >>>>>>>   " + second;
        }
    }

}
