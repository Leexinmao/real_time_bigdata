package com.realtime.flink.flinktable;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.source.SensouSoure;
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
 * @description: 流式表
 * @author: lxm
 * @create: 2020-11-26 20:29
 **/

public class StreamTable {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        fsEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, settings);


        DataStreamSource<SensorReading> streamSource = fsEnv.addSource(new SensouSoure());



        tableEnv.createTemporaryView("myTable", streamSource, $("id"), $("timestamp"),$("temperature"));



        Table tableSql = tableEnv.sqlQuery("select  id,temperature from myTable where id = 'sensor_1'");
        Table tableSql1 = tableEnv.sqlQuery("select  id,count(id) from myTable group by id ");
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(tableSql1, Row.class);



        tuple2DataStream.print();

        DataStream<Row> dsRow = tableEnv.toAppendStream(tableSql, Row.class);

        fsEnv.execute("sql");




    }

}
