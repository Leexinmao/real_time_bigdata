package com.realtime.flink.sink;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.model.SensouSoure;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-17 09:58
 **/
public class KafkaSink {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> stream = env.addSource(new SensouSoure());

        stream.addSink(new FlinkKafkaProducer011<SensorReading>(
                "hadoop:9092",
                "test",
                (KeyedSerializationSchema<SensorReading>) new SimpleStringSchema()
        ));

    }

    public static DataStreamSink<String> KafkaSource(DataStream<String> stream , String server, String topic){

        DataStreamSink<String> stringDataStreamSink = stream.addSink(new FlinkKafkaProducer011<String>(
                server,
                topic,
                new SimpleStringSchema()
        ));

        return stringDataStreamSink;
    }

    public static DataStreamSink<Row> Row(DataStream<Row> stream , String server, String topic){


        return null;


    }


}
