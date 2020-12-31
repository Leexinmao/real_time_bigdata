package com.realtime.flink.flinktable;


import com.bigdata.flink.admin.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-25 18:06
 **/
public class TableSqlApiTest1 {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        DataStream<SensorReading> ds = env.fromElements(
                new SensorReading("sensor_1", 10000L, 25.32),
                new SensorReading("sensor_2", 20000L, 25.38),
                new SensorReading("sensor_1", 30000L, 26.37),
                new SensorReading("sensor_3", 40000L, 24.55),
                new SensorReading("sensor_4", 50000L, 23.72),
                new SensorReading("sensor_2", 60000L, 24.52),
                new SensorReading("sensor_5", 70000L, 26.62),
                new SensorReading("sensor_3", 80000L, 27.12),
                new SensorReading("sensor_6", 90000L, 21.38),
                new SensorReading("sensor_5", 100000L, 20.39),
                new SensorReading("sensor_7", 110000L, 20.72),
                new SensorReading("sensor_1", 120000L, 21.52)
        );

        Table table = tableEnv.fromDataStream(ds, $("id"), $("timestamp"), $("temperature"));

        // 使用 SQL 查询内联的（未注册的）表
        Table result = tableEnv.sqlQuery(
                "SELECT SUM(temperature) FROM " + table + " WHERE id LIKE '%sensor_1%'");

//        result.execute().print();

        // SQL 查询一个已经注册的表
        // 根据视图 "Orders" 创建一个 DataStream
        tableEnv.createTemporaryView("sensor", ds, $("id"), $("temperature"), $("timestamp"));

        Table result2 = tableEnv.sqlQuery("SELECT id FROM sensor WHERE id LIKE '%sensor_1%'");

//        final Schema schema = new Schema()
//                .field("id", DataTypes.STRING())
//                .field("timestamp", DataTypes.BIGINT())
//                .field("temperature", DataTypes.DOUBLE());


//        tableEnv.connect(new FileSystem().path("D:\\bigdata\\table"))
////                .withFormat(new FormatDescriptor())
//                .withSchema(schema)
//                .createTemporaryTable("RubberOrders");
//
//        tableEnv.executeSql(
//                "INSERT INTO RubberOrders SELECT id, timestamp,temperature FROM sensor WHERE id LIKE '%sensor_1%'");

        env.execute();
    }

}
