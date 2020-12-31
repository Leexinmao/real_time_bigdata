package com.realtime.flink.flinktable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.OldCsv;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 19:45
 **/
public class ReadKafkaTable {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        tableEnv.connect(new Kafka()
                .version("")
                .topic("")
                .property("zookeeper.connect","hadoop110:2181")
                .property("bootstrap.servers","hadoop110:9092")
        ).withFormat(new OldCsv());



    }


}
