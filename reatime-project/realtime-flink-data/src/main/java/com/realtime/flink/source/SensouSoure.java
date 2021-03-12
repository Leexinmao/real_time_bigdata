package com.realtime.flink.source;


import com.realtime.flink.model.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-16 15:36
 **/
public class SensouSoure extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {

        while (running){
            for (int i = 1; i < 11; i++) {
                double v = Math.random() * 50 + 20;
                long timeInMillis = Calendar.getInstance().getTimeInMillis();
                ctx.collect(new SensorReading(("sensor_"+i),timeInMillis,v));
            }
            Thread.sleep(100);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}
