package com.realtime.flink.flinktable;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.model.SensouSoure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-19 13:12
 **/
public class FromDataStreamSql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        fsEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, settings);


        DataStreamSource<SensorReading> streamSource = fsEnv.addSource(new SensouSoure());

        Table table = tableEnv.fromDataStream(streamSource, $("id"), $("timestamp").as("ts"), $("temperature").as("temp"), $("pt").proctime());


        Table result = tableEnv.sqlQuery("SELECT SUM(temp) FROM " + table + " WHERE id LIKE '%sensor%'");

        tableEnv.createTemporaryView("Orders", streamSource, $("id"), $("timestamp").as("ts"), $("temperature"));
        // 在表上执行 SQL 查询并得到以新表返回的结果
        Table result2 = tableEnv.sqlQuery( "SELECT id,ts,temperature FROM Orders WHERE id LIKE '%sensor%'");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream1 = tableEnv.toRetractStream(result2, Row.class);
        tuple2DataStream1.print();

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(result, Row.class);
//        tuple2DataStream.print();


        fsEnv.execute(" ajsfld");


    }
}
