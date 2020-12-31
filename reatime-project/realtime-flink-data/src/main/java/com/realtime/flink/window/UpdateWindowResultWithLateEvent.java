package com.realtime.flink.window;


import com.bigdata.flink.admin.source.KafkaSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description: 处理迟到元素  单独开个窗户
 * 当迟到元素在allowed lateness时间内到达时，这个迟到元素会被实时处理并发送到触发器(trigger)。当水位线没过了窗口结束时间+allowed lateness时间时，窗口会被删除，并且所有后来的迟到的元素都会被丢弃。
 * @author: lxm
 * @create: 2020-11-20 15:06
 **/
public class UpdateWindowResultWithLateEvent {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> stream = KafkaSource.KafkaSource(env,"hadoop110:9092","test_kafka");

        stream.map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 5s的延迟时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                        return stringLongTuple2.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new UpdateWindowResult())
                .print();

        env.execute("UpdateWindowResultWithLateEvent1");

    }

    public static class UpdateWindowResult extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        private ValueState<Boolean> isUpdate = null;

        private ValueState<Long> timer = null;

        private ListState<Tuple2<String, Long>> listTuple2 = null;

        @Override
        public void open(Configuration parameters) throws Exception {

            isUpdate = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("value-update1",Boolean.class));

            timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state1", Long.class ));
            listTuple2 =  getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("list-state", Types.TUPLE(Types.STRING, Types.LONG)));

        }

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {

            iterable.forEach(r->{
                try {
                    listTuple2.add(r);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            for (Tuple2<String, Long> tr:listTuple2.get()){
                System.out.println(tr.f0  +"   ===  " + tr.f1);
            }
            long count = iterable.spliterator().getExactSizeIfKnown(); // 个数
            // 可见范围比getRuntimeContext.getState更小，只对当前key、当前window可见
            // 基于窗口的状态变量，只能当前key和当前窗口访问


            if (timer.value() == null ){
                Long value = timer.value();
                System.out.println(value);
                timer.update(context.currentProcessingTime());
            }

            // 当水位线超过窗口结束时间时，触发窗口的第一次计算！
            Boolean value = isUpdate.value();

            System.out.println(value);
            if (isUpdate.value() == null) {
                collector.collect("窗口第一次触发计算！一共有 " + count + " 条数据！"  );
                isUpdate.update(true);
                Boolean value1 = isUpdate.value();
                System.out.println(value1);
            } else {
                collector.collect("窗口更新了！一共有 " + count + " 条数据！");
            }

        }
    }
}
