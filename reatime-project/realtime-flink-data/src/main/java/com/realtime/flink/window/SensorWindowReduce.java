package com.realtime.flink.window;

import com.bigdata.flink.admin.model.SensorReading;
import com.bigdata.flink.admin.source.SensouSoure;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-18 16:52
 **/
public class SensorWindowReduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensouSoure());

        DataStream<String> process = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading, long l) {
                        return sensorReading.getTimestamp();
                    }
                })
        ).keyBy(r -> r.getId())
                .timeWindow(Time.seconds(10))
                .process(new WindowProcessResultss());

        process.print();
        env.execute("test1q11");

    }

    public static class  WindowProcessResultss extends ProcessWindowFunction<SensorReading,String,String, TimeWindow> {


        ListState<SensorReading> listState = null;

        ValueState<Long> timer = null;

        @Override
        public void open(Configuration parameters) throws Exception {

            listState = getRuntimeContext().getListState(new ListStateDescriptor<SensorReading>("list-state", SensorReading.class ));

        }

        @Override
        public void process(String s, Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
            long exactSizeIfKnown = elements.spliterator().getExactSizeIfKnown();
            out.collect(new Timestamp(context.window().getStart())+ " 到 ——————  " + new Timestamp(context.window().getEnd())+ "总共有 "+ exactSizeIfKnown +" 个元素");


        }

    }

}
