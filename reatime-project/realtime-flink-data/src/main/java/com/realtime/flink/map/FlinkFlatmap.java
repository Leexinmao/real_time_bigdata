package com.realtime.flink.map;

import com.realtime.flink.map.model.WordsCount;
import com.realtime.flink.uv.FlinkUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class FlinkFlatmap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        DataStreamSource<String> codeStream = env.fromElements("Takes one element and produces one element. A map function that doubles the values of the input stream",
                "DataStream<Integer> dataStream = //...",
                "dataStream.map(new MapFunction<Integer, Integer>() {",
                "test",
                "@Override",
                "public Integer map(Integer value) throws Exception {",
                "return 2 * value;",
                "}",
                "});"
        );

        codeStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String str : s) {
                    System.out.println();
                    out.collect(str);
                }
            }
        }).map(new MapFunction<String,  Tuple2<String,Long>>() {
            @Override
            public  Tuple2<String,Long> map(String value) throws Exception {
//                Tuple2<String,Integer>(value1.f0, value1.f1 + value2.f1);
                return  new   Tuple2<String,Long>(value,1L);
            }
        }).keyBy(r->r.f0).sum(1).print();

        env.execute("flink-flatmap");

    }
}
