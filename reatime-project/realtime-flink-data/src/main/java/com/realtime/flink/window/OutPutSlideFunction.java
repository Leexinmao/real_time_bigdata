package com.realtime.flink.window;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.model.SensouSoure;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @description: 当温度小于32度后侧输入流
 * @author: lxm
 * @create: 2020-11-22 23:01
 **/
public class OutPutSlideFunction {

    private static OutputTag<String> output = new OutputTag<String>("side-output") {};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<SensorReading> stream = env.addSource(new SensouSoure())
                .process(new MyOutputProcess());

        stream.print();

        stream.getSideOutput(output).print();

        env.execute("OutPutSlideFunction");

    }

    public static class MyOutputProcess extends ProcessFunction<SensorReading, SensorReading> {

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (value.getTemperature() < 32) {
                ctx.output(output, "温度小于32度！");
            }else {
                out.collect(value);
            }
        }
    }

}
