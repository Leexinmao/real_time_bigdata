package com.realtime.flink.window;

import com.bigdata.flink.admin.source.KafkaSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-20 14:40
 **/
public class WindowWaterLineTest {


    private static OutputTag<Tuple2<String, Long>> output = new OutputTag<Tuple2<String, Long>>("late-readings"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> stream = KafkaSource.KafkaSource(env, "hadoop110:9092", "test_kafka")
        .map(r -> {
            String[] split = r.split(" ");
            if (split.length >= 2) {

                return Tuple2.of(split[0], Long.parseLong(split[1]) * 1000);
            } else {
                return null;
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> value, long l) {
                                        return value.f1;
                                    }
                                })
                );

        SingleOutputStreamOperator<String> lateReadings = stream
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .sideOutputLateData(output) // use after keyBy and timeWindow
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        long exactSizeIfKnown = iterable.spliterator().getExactSizeIfKnown();
                        collector.collect(exactSizeIfKnown + " of elements");
                    }
                });

        lateReadings.print();
        lateReadings.getSideOutput(output).print();

        env.execute();
    }
}
