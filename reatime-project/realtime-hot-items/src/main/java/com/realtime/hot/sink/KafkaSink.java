package com.realtime.hot.sink;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-17 09:58
 **/
public class KafkaSink {


    public static DataStreamSink<String> KafkaSource(DataStream<String> stream , String server, String topic){

        DataStreamSink<String> stringDataStreamSink = stream.addSink(new FlinkKafkaProducer011<String>(
                server,
                topic,
                new SimpleStringSchema()
        ));

        return stringDataStreamSink;
    }

}
