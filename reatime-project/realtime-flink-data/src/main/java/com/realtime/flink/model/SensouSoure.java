package com.realtime.flink.model;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-12 22:22
 **/
public class SensouSoure  extends RichParallelSourceFunction<SensorReading> {

    private  boolean  running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {


        while (running){
            List<String > stringList = new ArrayList<>();
            for (int j = 0; j < 20; j++) {
                double v = Math.random() * 30 + 20;
                stringList.add("sensor_"+j);
                long timeInMillis = Calendar.getInstance().getTimeInMillis();
                ctx.collect(new SensorReading(("sensor_"+j),timeInMillis,v));
                Thread.sleep(500);
            }
        }


    }

    @Override
    public void cancel() {
            running = false;
    }
}
