package com.realtime.flink.flinktable;


import com.realtime.flink.model.SensorReading;
import com.realtime.flink.source.SensouSoure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
 * @create: 2020-12-21 09:34
 **/
public class SumTableFlink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        DataStreamSource<SensorReading> streamSource = env.addSource(new SensouSoure());

//        tableEnv.createTemporaryView("source",streamSource,$("id"),$("timestamp") ,$("temperature"),$("pt").proctime());
        tableEnv.createTemporaryView("source",streamSource,$("id"),$("timestamp"),$("temperature"),$("proctime").proctime(), $("rowtime").rowtime());

//        Table tumble = tableEnv.sqlQuery("select id,count(id) from source group by  TUMBLE(timestamp,INTERVAL '2' SECOND), id"); // 滚动窗口
        Table hop = tableEnv.sqlQuery("select id,sum(temperature) from source group by HOP(rowtime, INTERVAL '2' SECOND, INTERVAL '10' SECOND), id"); // 滑动窗口

        DataStream<Tuple2<Boolean, Row>> tuple1 = tableEnv.toRetractStream(hop, Row.class);
        tuple1.print("sum");

//        Table table1 = tableEnv.sqlQuery("select id,temperature from source ");
////        DataStream<Tuple2<Boolean, Row>> tuple2 = tableEnv.toRetractStream(table1, Row.class);
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table1, Row.class);
//        rowDataStream.print("*");

        env.execute("SumTableFlink");


    }

}
