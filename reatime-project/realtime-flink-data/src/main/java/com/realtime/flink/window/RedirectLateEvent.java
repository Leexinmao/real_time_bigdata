package com.realtime.flink.window;


import com.realtime.flink.source.KafkaSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-20 14:45
 **/
public class RedirectLateEvent {

    private static OutputTag<String> output = new OutputTag<String>("late-readings"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = KafkaSource.KafkaSource(env, "hadoop110:9092", "test_kafka")
                .map(r -> {
                    String[] split = r.split(" ");
                    if (split.length >= 2) {

                        return Tuple2.of(split[0], Long.parseLong(split[1]) * 1000);
                    } else {
                        return null;
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> value, long l) {
                                        return value.f1;
                                    }
                                })
                )
                .process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                        if (stringLongTuple2.f1 < context.timerService().currentWatermark()) {
                            context.output(output, "迟到元素来了!");
                        } else {
                            collector.collect(stringLongTuple2);
                        }

                    }
                });

        stream.print();
        stream.getSideOutput(output).print();

        env.execute();
    }
}
