package com.realtime.flink.window;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.model.SensouSoure;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @description: 检测温度连续1s上升后报警
 * @author: lxm
 * @create: 2020-11-22 22:35
 **/
public class TempIncreaseAlert {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SensouSoure())
                .keyBy(r->r.getId())
                .process(new TempProcessAlert())
                .print();

        env.execute("TempIncreaseAlert");

    }

    public static class TempProcessAlert extends KeyedProcessFunction<String, SensorReading,String > {


        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

        }
    }
}
