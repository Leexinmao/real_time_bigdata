package com.realtime.flink.sink;


import com.realtime.flink.model.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-21 20:02
 **/
public class CustomAndToKafkaSink extends RichSinkFunction<SensorReading> {



    public static class CusTomToKafkaSource extends RichSinkFunction<SensorReading>{


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }



        @Override
        public void close() throws Exception {
            super.close();
        }

    }


}
