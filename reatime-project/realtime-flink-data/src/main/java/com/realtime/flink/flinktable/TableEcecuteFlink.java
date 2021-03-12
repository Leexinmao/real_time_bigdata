package com.realtime.flink.flinktable;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.source.SensouSoure;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-19 13:39
 **/
public class TableEcecuteFlink {

    public static void main(String[] args) {

        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        fsEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, settings);


        DataStreamSource<SensorReading> streamSource = fsEnv.addSource(new SensouSoure());

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().getConfiguration().set( ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));

        tableEnv.executeSql("CREATE TABLE Orders (`id` STRING, timestamp BIGINT, temperature DOUBLE) WITH (...)");
    }

}
