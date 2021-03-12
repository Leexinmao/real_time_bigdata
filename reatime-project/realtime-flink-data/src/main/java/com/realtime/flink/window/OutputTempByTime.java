package com.realtime.flink.window;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.model.SensouSoure;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-24 16:16
 **/
public class OutputTempByTime {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> stream = env.addSource(new SensouSoure());

        stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                })).keyBy(r->r.getId())
                .timeWindow(Time.seconds(10))
                .process(new MyOutputTempFunction())
                .print();


        env.execute("OutputTempByTime");

    }

//     ProcessWindowFunction<Tuple2<String,Long>,String,String, TimeWindow>
    public static class MyOutputTempFunction extends ProcessWindowFunction<SensorReading,String,String, TimeWindow>{


    @Override
    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {

    }
}


}
