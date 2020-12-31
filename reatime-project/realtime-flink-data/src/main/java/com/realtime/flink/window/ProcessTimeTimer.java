package com.realtime.flink.window;

import com.bigdata.flink.admin.source.KafkaSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @description: 机器时间定时任务
 * @author: lxm
 * @create: 2020-11-22 21:29
 **/
public class ProcessTimeTimer {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> stream = KafkaSource.KafkaSource(env,"hadoop110:9092", "bigdata_event");

        stream.map(line -> {
            String[] split = line.split(" ");
            return Tuple2.of(split[0], Long.valueOf(split[1]) * 1000L);
        }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(k -> k.f0)
                .process(new MyKeyByProcessTImer())
                .print();

        env.execute("EventTimeTimer");

    }

    public static class MyKeyByProcessTImer extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {


        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect("定时器执行了  时间是" + new Timestamp(timestamp));
        }
    }

}