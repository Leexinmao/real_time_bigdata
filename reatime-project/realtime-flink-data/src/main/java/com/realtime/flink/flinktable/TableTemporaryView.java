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
 * @description: 通过createTemporaryView来创建临时表
 * @author: lxm
 * @create: 2020-11-27 15:14
 **/
public class TableTemporaryView {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStreamSource<SensorReading> stream = streamEnv.addSource(new SensouSoure());

        tableEnv.createTemporaryView("sensor",stream,$("id"),$("timestamp"),$("temperature"),$("pt").proctime());  // 设置字段

        Table table = tableEnv.sqlQuery("select id,pt from sensor");

        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(table, Row.class);

        resultStream.print();

        streamEnv.execute("TableTemporaryView");


    }


}
