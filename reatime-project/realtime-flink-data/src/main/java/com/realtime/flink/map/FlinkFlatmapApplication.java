package com.realtime.flink.map;

import com.realtime.flink.map.model.WordsCount;
import com.realtime.flink.model.ItemViewCount;
import com.realtime.flink.uv.FlinkUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FlinkFlatmapApplication {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        DataStreamSource<String> codeStream = env.fromElements("Takes one element and produces one element. A map function that doubles the values of the input stream",
                "DataStream<Integer> dataStream = //...",
                "dataStream.map(new MapFunction<Integer, Integer>() {",
                "@Override",
                "public Integer map(Integer value) throws Exception {",
                "return 2 * value;",
                "}",
                "});"
        );

        SingleOutputStreamOperator<WordsCount> map = codeStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String str : s) {
                    out.collect(str);
                }
            }
        }).map(new MapFunction<String, WordsCount>() {
            @Override
            public WordsCount map(String value) throws Exception {
                return new WordsCount(value, 1L);
            }
        });
//        map.print();

        map.keyBy(r->r.getName()).timeWindow(Time.milliseconds(5)).aggregate(new MyAggregate(),new MyResultFunction()).print();

        env.execute("flatmapFLink");


    }


    public static class MyAggregate implements AggregateFunction<WordsCount,Long,Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(WordsCount value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    public static class MyResultFunction extends ProcessWindowFunction<Long,String,String, TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
//            Long next = iterable.iterator().next(); // 点击数量 AggCount中累加
//            collector.collect(new ItemViewCount(aLong, context.window().getEnd(), next));

            Long next = elements.iterator().next();
            out.collect(s+" 总共有； "+ next +" 个" );
        }
    }


}
