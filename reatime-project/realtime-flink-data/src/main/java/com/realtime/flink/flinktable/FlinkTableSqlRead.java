package com.realtime.flink.flinktable;

import com.bigdata.flink.admin.model.SensorReading;
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
 * @description: table api 和 sql
 * @author: lxm
 * @create: 2020-11-25 17:14
 **/
public class FlinkTableSqlRead {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.fromElements(
                new SensorReading("sensor_1",10000L,25.32),
                new SensorReading("sensor_2",20000L,25.38),
                new SensorReading("sensor_1",30000L,26.37),
                new SensorReading("sensor_3",40000L,24.55),
                new SensorReading("sensor_4",50000L,23.72),
                new SensorReading("sensor_2",60000L,24.52),
                new SensorReading("sensor_5",70000L,26.62),
                new SensorReading("sensor_3",80000L,27.12),
                new SensorReading("sensor_6",90000L,21.38),
                new SensorReading("sensor_5",100000L,20.39),
                new SensorReading("sensor_7",110000L,20.72),
                new SensorReading("sensor_1",120000L,21.52)
        );



//        tables.printSchema();



        tableEnv.createTemporaryView("sensor_reading",stream,$("id"),$("timestamp").as("ts"),$("temperature").as("temp"),$("pt").proctime());


        Table tableSql = tableEnv.sqlQuery("select id,count(id) from sensor_reading group by id ");
//        Table tableSql = tableEnv.sqlQuery("select id,timestamp,temperature from sensor_reading where id = 'sensor_1'");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(tableSql, Row.class);

        DataStream<SensorReading> stream1 = tableEnv.toAppendStream(tableSql, SensorReading.class);

        stream1.print();

//        Table table = tableEnv.fromDataStream(stream,"id,timestamp,temperature");
//
//        String id = table.select("id").toString();
//        System.out.println(id);

        env.execute("FlinkTableSql");


    }
}
