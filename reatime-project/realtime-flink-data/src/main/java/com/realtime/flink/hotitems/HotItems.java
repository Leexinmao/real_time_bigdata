package com.realtime.flink.hotitems;


import com.realtime.flink.model.ItemViewCount;
import com.realtime.flink.model.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-27 17:04
 **/
public class HotItems {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.readTextFile("D:\\bigdate\\gitproject\\real_time_bigdata\\reatime-project\\realtime-flink-data\\src\\main\\resources\\UserBehavior.csv");

        DataStream<UserBehavior> map = stream.map(line -> {
            String[] str = line.split(",");
            // Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp
            return new UserBehavior(Long.parseLong(str[0]), Long.parseLong(str[1]), Integer.parseInt(str[2]), str[3], Long.parseLong(str[4]) * 1000L);
        });
        map.filter(r -> r.behavior.equals("pv"));

//        DataStream<UserBehavior> withTimestampsAndWatermarks = map.assignTimestampsAndWatermarks(new MyAssignTimeStampsAndWaterMarks());

//        SingleOutputStreamOperator<ItemViewCount> aggregate = withTimestampsAndWatermarks.keyBy(r -> r.itemId)
//                .timeWindow(Time.minutes(60), Time.minutes(5))
//                // AggCount 算出group by itemId的数量
//                .aggregate(new AggCount(), new WindowResultFunction());
//        aggregate.keyBy(r -> r.getWindowEnd())
//                .process(new ItemTop(5))
//                .print();


        DataStream<ItemViewCount> pv = stream.map(line -> {
            String[] str = line.split(",");
            // Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp
            System.out.println();
            return new UserBehavior(Long.parseLong(str[0]), Long.parseLong(str[1]), Integer.parseInt(str[2]), str[3], Long.parseLong(str[4]) * 1000L);
        })
                .filter(r -> r.behavior.equals("pv"))

//                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(10))
//                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
//                    @Override
//                    public long extractTimestamp(UserBehavior userBehavior, long l) {
//                        return l;
//                    }
//                }))

                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior userBehavior, long l) {
                                System.out.println();
                                return userBehavior.timestamp;
                            }
                        }))
                .keyBy(r -> r.itemId)
                .timeWindow(Time.minutes(60), Time.minutes(5))
                // AggCount 算出group by itemId的数量
                .aggregate(new AggCount(), new WindowResultFunction());
        /* TODO 以时间戳结束时间分流 可以是在相同时间范围的数据在同一条流输出 */
        pv.keyBy(r -> r.getWindowEnd())
                .process(new ItemTop(5))
                .print();

        env.execute("HotItemss");


    }

    //    public static class AggCount extends AggregateAggFunction<UserBehavior,Long,Long> {
    public static class AggCount implements AggregateFunction<UserBehavior, Long, Long> {
        // 第一个参数为输入  第二个参数为累加器 表示这个商品点击了多少次  第三个为输出

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1 + aLong;
        }
    }

    public static class WindowResultFunction extends ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow> {


        @Override
        public void process(Long aLong, Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {

            Long next = iterable.iterator().next(); // 点击数量 AggCount中累加


            collector.collect(new ItemViewCount(aLong, context.window().getEnd(), next));
        }
    }

    public static class ItemTop extends KeyedProcessFunction<Long, ItemViewCount, String> {

        public Integer topN;

        private ListState<ItemViewCount> itemState;

        public ItemTop(Integer topN) {
            this.topN = topN;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-state", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            itemState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterable<ItemViewCount> itemViewCounts = itemState.get();

            List<ItemViewCount> list = new ArrayList<>();

            for (ItemViewCount itemViewCount : itemViewCounts) {
                list.add(itemViewCount);
            }
            itemState.clear(); // 清空

            List<ItemViewCount> collect = list.stream().sorted(Comparator.comparing(ItemViewCount::getCount).reversed()).collect(Collectors.toList());


            StringBuilder result = new StringBuilder();
            result.append("============== 商品热销榜 ===================\n")
                    .append("时间为: ")
                    .append(new Timestamp(timestamp - 100L));

            for (int i = 0; i < this.topN; i++) {
                ItemViewCount currItem = collect.get(i);
                result.append("  \n第")
                        .append(i + 1)
                        .append("名商品id是 : ")
                        .append(currItem.getItemId())
                        .append("  点击量为： ")
                        .append(currItem.getCount())
                        .append("\n");
            }
            result.append("===========================================\n\n\n");
            out.collect(result.toString());
        }
    }
}