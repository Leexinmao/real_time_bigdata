package com.realtime.flink.flinktable;

import com.bigdata.flink.admin.model.SensorReading;
import com.bigdata.flink.admin.source.SensouSoure;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @description: flink table 滑动窗口 使用处理时间 table api写sql
 * @author: lxm
 * @create: 2020-11-27 15:00
 **/
public class TableSlideWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();// inStreamingMode表示流  inBatchMode表示批

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStreamSource<SensorReading> stream = streamEnv.addSource(new SensouSoure());

//        tableEnv.createTemporaryView("sensor",stream,$("id"),$("timestamp").as("ts"),$("temperature"),$("ts"));

        Table table = tableEnv.fromDataStream(stream, $("id"), $("timestamp").as("ts"), $("temperature").as("temp"), $("pt").proctime());

        Table select = table.window(Slide.over(lit(10).seconds()).every(lit(5).seconds())
                .on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("id").count());

        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(select, Row.class);

        resultStream.print();

        streamEnv.execute("TableSlideWindow");



    }
}
