package com.realtime.flink.flinktable;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.model.SensouSoure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @description: table 开窗
 * @author: lxm
 * @create: 2020-11-26 23:39
 **/
public class TableWIndow {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStreamSource<SensorReading> stream = streamEnv.addSource(new SensouSoure());

        // proctime表示处理时间
        Table table = tableEnv.fromDataStream(stream, $("id"), $("timestamp").as("ts"), $("temperature").as("temp"), $("pt").proctime());
        // 开窗

        Table table1 = table.window(Tumble.over(lit(10).seconds())
                .on($("pt")).as("w"))  // 10s为一个窗口  以处理时间为准
                // 类似 keyby("id").timeWindow(Times.seconds(10))
                .groupBy($("id"), $("w"))
                .select($("id"), $("id").count());
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(table1, Row.class);

        resultStream.print();

        streamEnv.execute("TableWIndow");

    }

}
