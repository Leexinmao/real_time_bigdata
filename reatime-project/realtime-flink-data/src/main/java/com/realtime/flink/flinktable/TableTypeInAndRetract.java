package com.realtime.flink.flinktable;

import com.bigdata.flink.admin.model.SensorReading;
import com.bigdata.flink.admin.source.SensouSoure;
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
 * @create: 2020-11-26 23:20
 **/
public class TableTypeInAndRetract {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStreamSource<SensorReading> stream = streamEnv.addSource(new SensouSoure());

        tableEnv.createTemporaryView("sensor",stream,$("id"),$("timestamp").rowtime(),$("temperature")); // rowtime表示为事件时间

        Table table = tableEnv.sqlQuery("select id,count(id) from sensor where id = 'sensor_1' group by id");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);
        tuple2DataStream.print();

        streamEnv.execute("TableTypeInAndRetract ");


    }


}
