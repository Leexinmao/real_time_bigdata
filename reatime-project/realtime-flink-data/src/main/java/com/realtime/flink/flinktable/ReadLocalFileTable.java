package com.realtime.flink.flinktable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 19:30
 **/
public class ReadLocalFileTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        String path = "D:\\bigdata\\javaflink\\mavenproject\\file\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE())
                ).createTemporaryTable("sensor");

        Table sensor = tableEnv.from("sensor");

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(sensor, Row.class);
        rowDataStream.print();


        env.execute("ReadLocalFileTable");


    }

}
