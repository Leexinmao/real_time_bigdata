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

import java.sql.Timestamp;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-18 22:25
 **/
public class WindowAndWaterline {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        env.setParallelism(1);

//        env.getConfig().

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> stream = KafkaSource.KafkaSource(env, "hadoop110:9092", "test_kafka");
        SingleOutputStreamOperator<String> process = stream.map(r -> {
            String[] split = r.split(" ");
            if (split.length >= 2) {

                return Tuple2.of(split[0], Long.parseLong(split[1]) * 1000);
            } else {
                return null;
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {

                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }))
//                  老版本
//                 .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(5)) {
//                     @Override
//                     public long extractTimestamp(Tuple2<String, Long> element) {
//                         return  element.f1;
//                     }
//                 })
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(10))
                .process(new WindowProcessResult());// 全窗口聚合

        process.print();

        env.execute("testa window");
    }

    public static class  WindowProcessResult extends ProcessWindowFunction<Tuple2<String,Long>,String,String, TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

            long exactSizeIfKnown = elements.spliterator().getExactSizeIfKnown();
//            Iterator<Tuple2<String, Long>> it = elements.iterator();
//            Integer size = 0;
//            while (it.hasNext()) {
//                it.next();
//                size++;
//            }
            out.collect(new Timestamp(context.window().getStart())+ " 到 ——————  " + new Timestamp(context.window().getEnd())+ "总共有 "+ exactSizeIfKnown +" 个元素");
        }
    }


}
