package com.realtime.flink.flinktable;

import com.bigdata.flink.admin.model.SensorReading;
import com.bigdata.flink.admin.source.SensouSoure;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @description: flink table 滑动窗口
 * @author: lxm
 * @create: 2020-11-27 00:53
 **/
public class EventTImeTableWindow {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        DataStreamSource<SensorReading> stream = streamEnv.addSource(new SensouSoure());

        // proctime表示处理时间
        Table table = tableEnv.fromDataStream(stream, $("id"), $("timestamp").as("ts"), $("temperature").as("temp"), $("pt").proctime());

        Table table2 = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("name", DataTypes.STRING())
                ),
                row(1, "ABC"),
                row(2L, "ABCDE")
        );
        Table orders = tableEnv.from("orders");
        Table result = orders.select($("a"), $("c").as("d"));
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);
        rowDataStream.print();


        Table table3 = table2.window(Slide.over(lit(10).seconds()).every(lit(5).seconds())
                .on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("id").count());
        DataStream<Row> rowDataStream1 = tableEnv.toAppendStream(table3, Row.class);
        rowDataStream1.print();


//        tableEnv.createTemporaryView();

        // 开窗 滑动窗口 10s的窗口  每5s滑动一次
//        Table table1 = table.window(Slide.over(lit(10).seconds()).every(lit(5).seconds())
//                .on($("pt")).as("w"))
//                .groupBy($("id"), $("w"))
//                .select($("id"), $("id").count());
//        Table select = table.select($("id"), $("ts"), $("temp"), $("pt"));
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(select, Row.class);
//



//        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(table1, Row.class);
//        DataStream<Row> tuple2DataStream = tableEnv.toAppendStream(table1, Row.class);

//        resultStream.print();

//        rowDataStream.print();
        streamEnv.execute("EventTImeTableWindow");

    }
}
