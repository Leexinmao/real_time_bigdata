package com.realtime.hot.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-17 10:40
 **/
public class KafkaSource {


    /**
     *
     * @param env flink执行环境
     * @param server kafka的ip地址
     * @param topic 主题
     * @return
     */
    public static DataStream<String> KafkaToSource(StreamExecutionEnvironment env,String server,String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id",groupId);
        properties.setProperty(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty(
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty("auto.offset.reset", "latest");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> stream = env
                // source为来自Kafka的数据，topic为自定义的kafka主题
                .addSource(
                        new FlinkKafkaConsumer011<String>(
                                topic,
                                new SimpleStringSchema(),
                                properties
                        )
                );
        return stream;
    }


    public static DataStream<String> KafkaToSource(StreamExecutionEnvironment env){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop110:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty(
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty("auto.offset.reset", "latest");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> stream = env
                // source为来自Kafka的数据，topic为自定义的kafka主题
                .addSource(
                        new FlinkKafkaConsumer011<String>(
                                "test_kafka",
                                new SimpleStringSchema(),
                                properties
                        )
                );

        return stream;
    }


    public static DataStream<String> KafkaSource(StreamExecutionEnvironment env,String server,String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id",groupId);
        properties.setProperty(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty(
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );
        properties.setProperty("auto.offset.reset", "latest");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> stream = env
                // source为来自Kafka的数据，topic为自定义的kafka主题
                .addSource(
                        new FlinkKafkaConsumer011<String>(
                                topic,
                                new SimpleStringSchema(),
                                properties
                        )
                );

        return stream;
    }

}
