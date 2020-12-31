package com.realtime.hot.calc;

import com.realtime.hot.goods.sink.HotItemsTopMysqlSink;
import com.realtime.hot.goods.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-29 17:49
 **/
public class HotItemsTopToMysql {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> stream = KafkaSource.KafkaToSource(env, "hadoop110:9092", "hot_items_top", "hot_goods_top");


        stream.addSink(new HotItemsTopMysqlSink());
//        stream.print();

        env.execute("HotItemsTopToMysql1");

    }


}
