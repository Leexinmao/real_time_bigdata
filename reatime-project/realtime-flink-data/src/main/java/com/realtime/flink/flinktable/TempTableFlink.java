package com.realtime.flink.flinktable;

import com.bigdata.flink.admin.model.SensorReading;
import com.bigdata.flink.admin.source.SensouSoure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-21 14:38
 **/
public class TempTableFlink {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<SensorReading> source = env.addSource(new SensouSoure());

        tableEnv.createTemporaryView("Orders", source,$("id"), $("timestamp").as("ts"), $("temperature"), $("proctime").proctime() );
//        Table result1 = tableEnv.sqlQuery(
//                "SELECT id, max(ts) " +
//                        "  TUMBLE_START(proctime, INTERVAL '1' SECOND) as wStart,  " +
//                        "  SUM(temperature) FROM Orders " +
//                        "GROUP BY TUMBLE(proctime, INTERVAL '1' SECOND), id");
//
//        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(result1, Row.class);
//        tuple2DataStream.print();

        Table result2 = tableEnv.sqlQuery( "SELECT * FROM Orders ");
//
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream1 = tableEnv.toRetractStream(result2, Row.class);

        tuple2DataStream1.print();
        env.execute("TempTableFlink");

    }
}
