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
 * @description: 两条流联合后union的水位线
 * @author: lxm
 * @create: 2020-11-22 13:09
 **/
public class KeyByProcessFunction1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> stream1 = KafkaSource.KafkaSource(env, "hadoop110:9092", "bigdata_event")
                .map(line -> {
                    String[] split = line.split(" ");
                    return Tuple2.of(split[0], Long.parseLong(split[1]) * 1000);
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));

        DataStream<Tuple2<String, Long>> stream2 = KafkaSource.KafkaSource(env, "hadoop110:9092", "test_kafka")
                .map(line -> {
                    String[] split = line.split(" ");
                    return Tuple2.of(split[0], Long.parseLong(split[1]) * 1000);
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {

                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }));

        stream1.union(stream2)
                .keyBy(r -> r.f0)
                .process(new MyProcessKey())
                .print();

        env.execute("KeyByProess");

    }

    public static class MyProcessKey extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            out.collect("当前水位线时间戳是 " + ctx.timerService().currentWatermark());
        }
    }

}
