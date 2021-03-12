package com.realtime.flink.window;

import com.realtime.flink.source.KafkaSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-23 23:04
 **/
public class TriggerWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> stream = KafkaSource.KafkaSource(env); // topic test_kafka

        stream.map(line -> {
            String[] split = line.split(" ");
            return Tuple2.of(split[0], Long.parseLong(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
                ).keyBy(r -> r.f0)
                .timeWindow(Time.seconds(10))
                .trigger(new MyAggreGateFunction())
                .process(new MyProcessFunction())
                .print();

        env.execute("TriggerWindow");

    }

    public static class MyAggreGateFunction extends Trigger<Tuple2<String, Long>, TimeWindow> {

        @Override
        public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            // z这个方法是没来一个数据调用一次
            // 默认为false
            // 当第一条数据来的时候设置为true
            ValueState<Boolean> firstTeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first_trigger", Types.BOOLEAN)
            );

            // 当第一条数据来的时候才会执行  总共只会执行一次
            if (!firstTeen.value()) {
                // 如果当前水位线为1234 则 1234 + （ 1000 -1234 % 1000） 1234 +1000 - 234 = 2000
                long t = ctx.getCurrentWatermark() + (1000 - ctx.getCurrentWatermark() % 1000);

                System.out.println("  第一条数据来了 当前水位线是 " + ctx.getCurrentWatermark());
                ctx.registerEventTimeTimer(t);  // 注册一条定时器 开始一个定时器

                System.out.println(" 第一条数据的的注册的整数秒是  " + t);

                ctx.registerEventTimeTimer(window.getEnd()); // 窗口结束事件一个定时器

                firstTeen.update(true);

            }
            return TriggerResult.CONTINUE;


        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

            // 因为是事件时间  故处理时间就不做处理
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

            if (time == window.getEnd()) {
                // 窗口结束时  触发计算并清空窗口

                return TriggerResult.FIRE_AND_PURGE;

            } else {
                long t = ctx.getCurrentWatermark() + (1000 - ctx.getCurrentWatermark() % 1000);
                if (t < window.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                return TriggerResult.FIRE;
            }

        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> firstTeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first_trigger", Types.BOOLEAN)
            );

            firstTeen.clear();
        }
    }


    public static class MyProcessFunction extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {

            out.collect("窗口中有  " + elements.spliterator().getExactSizeIfKnown() + " 条 数据,c窗口结束时间为 " + context.window().getEnd());
        }
    }

}
