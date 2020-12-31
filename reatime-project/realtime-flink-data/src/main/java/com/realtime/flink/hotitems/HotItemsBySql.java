package com.realtime.flink.hotitems;

import com.bigdata.flink.userbehavior.model.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * @create: 2020-11-27 17:04
 **/
public class HotItemsBySql {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.readTextFile("D:\\bigdata\\javaflink\\mavenproject\\src\\main\\resources\\UserBehavior.csv");

        DataStream<UserBehavior> resultSource = stream.map(line -> {
            String[] str = line.split(",");
            return new UserBehavior(Long.parseLong(str[0]), Long.parseLong(str[1]), Integer.parseInt(str[2]), str[3], Long.parseLong(str[4]) * 1000L);
        })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                return userBehavior.timestamp;
                            }
                        }));

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

//        Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp
//        tableEnv.fromDataStream(resultSource,$("userId"),$("itemId"),$("categoryId"),$("behavior"),$("timestamp").rowtime(),$("pt").proctime());

        tableEnv.createTemporaryView("t_hot_items",resultSource,$("userId"),$("itemId"),$("categoryId"),$("behavior"),$("timestamp").rowtime().as("ts"),$("pt").proctime());

        Table table = tableEnv.sqlQuery("select itemId,count(itemId) from t_hot_items group by itemId");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);


        tuple2DataStream.print();

        env.execute("HotItems");


    }


}