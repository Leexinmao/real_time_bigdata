package com.realtime.flink.window;


import com.realtime.flink.source.KafkaSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description: 事件时间定时任务
 * @author: lxm
 * @create: 2020-11-22 19:39
 **/
public class EventTimeTimer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> stream = KafkaSource.KafkaSource(env,"hadoop110:9092","test_kafka");

        stream.map(line -> {
            String[] split = line.split(" ");
            return Tuple2.of(split[0], Long.valueOf(split[1]) * 1000L);
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                // 分配直接戳 指定哪个是事件时间
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }))

                .keyBy(k -> k.f0)
                .process(new MyKeyByEnventTImer())
                .print();

        env.execute("EventTimeTimer");

    }

    public static class MyKeyByEnventTImer extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {

            // 注册一个定时器  十秒后
            ctx.timerService().registerEventTimeTimer(value.f1 + 10 * 1000L);

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect("定时器执行了  时间是" + timestamp);
        }
    }

}
