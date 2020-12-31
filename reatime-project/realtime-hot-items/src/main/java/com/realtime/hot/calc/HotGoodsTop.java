package com.realtime.hot.calc;

import com.alibaba.fastjson.JSON;
import com.realtime.hot.model.GoodsClick;
import com.realtime.hot.model.GoodsTopItems;
import com.realtime.hot.model.GoodsViewCount;
import com.realtime.hot.sink.KafkaSink;
import com.realtime.hot.source.KafkaSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: 商品点击热度排名
 * @author: lxm
 * @create: 2020-12-29 14:16
 **/
public class HotGoodsTop {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStream<String> stream = KafkaSource.KafkaToSource(env, "hadoop110:9092", "hots", "hot_goods_top");

        KeyedStream<GoodsViewCount, Long> pv = stream.map(r -> {
            return JSON.parseObject(r, GoodsClick.class);
        }).filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<GoodsClick>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<GoodsClick>() {
                            @Override
                            public long extractTimestamp(GoodsClick goodsClick, long l) {
                                return goodsClick.timestamp;
                            }
                        }))
                .keyBy(r -> r.itemId)
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new AggAccount(), new WindowTopFunction())
                .keyBy(r -> r.getWindowEnd());

        DataStream<String> process = pv.process(new ItemsTop(20));

        KafkaSink.KafkaSource(process,"hadoop110:9092","hot_items_top");

        env.execute("HotGoodsTop");


    }

    public static class AggAccount implements AggregateFunction<GoodsClick, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(GoodsClick goodsClick, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class WindowTopFunction extends ProcessWindowFunction<Long, GoodsViewCount, Long, TimeWindow> {

        @Override
        public void process(Long aLong, Context context, Iterable<Long> elements, Collector<GoodsViewCount> out) throws Exception {

            Long next = elements.iterator().next();
            out.collect(new GoodsViewCount(aLong, context.window().getEnd(), next));

        }
    }

    public static class ItemsTop extends KeyedProcessFunction<Long, GoodsViewCount, String> {

        private Integer topNum;

        public ItemsTop(Integer topNum){
            this.topNum = topNum;
        }

        private ListState<GoodsViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {

            listState = getRuntimeContext().getListState(new ListStateDescriptor<GoodsViewCount>("items_top", GoodsViewCount.class));

        }

        @Override
        public void processElement(GoodsViewCount value, Context ctx, Collector<String> out) throws Exception {

            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+ 200L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            Iterable<GoodsViewCount> goodsViewCounts = listState.get();
            List<GoodsViewCount> lists = new ArrayList<>();

            for (GoodsViewCount goodsViewCount : goodsViewCounts) {
                lists.add(goodsViewCount);
            }
            List<GoodsViewCount> collect = lists.stream().sorted(Comparator.comparing(GoodsViewCount::getCount).reversed()).collect(Collectors.toList());
            for (int i = 0; i < topNum; i++) {
                GoodsViewCount goodsViewCount = collect.get(i);

                GoodsTopItems goodsTopItems = new GoodsTopItems(goodsViewCount.getItemId(), goodsViewCount.getCount(), goodsViewCount.getWindowEnd()-200, (i + 1));
                out.collect(JSON.toJSONString(goodsTopItems));
            }
            listState.clear(); // 清空

        }
    }

}
