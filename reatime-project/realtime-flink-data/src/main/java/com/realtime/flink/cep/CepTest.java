package com.realtime.flink.cep;



import com.realtime.flink.model.UserBehaviorLogIn;
import com.realtime.flink.model.UserBehaviorSource;
import com.realtime.flink.utils.DataUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 13:59
 **/
public class CepTest {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<UserBehaviorLogIn> source = env.addSource(new UserBehaviorSource());
        KeyedStream<UserBehaviorLogIn, String> userKeyStream = source.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehaviorLogIn>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehaviorLogIn>() {
                    @Override
                    public long extractTimestamp(UserBehaviorLogIn userBehaviorLogIn, long l) {
                        return userBehaviorLogIn.loginTime;
                    }
                })
        ).keyBy(r -> r.name);



        // 指定事件的泛型是UserBehaviorLogIn  指定事件的名字为first  可以随意指定
        Pattern<UserBehaviorLogIn, UserBehaviorLogIn> pattern = Pattern.<UserBehaviorLogIn>begin("first")
                // first事件需要满足的条件是
                .where(new SimpleCondition<UserBehaviorLogIn>() {
                    @Override
                    public boolean filter(UserBehaviorLogIn userBehaviorLogIn) throws Exception {
                        return userBehaviorLogIn.type.equals("fail");
                    }
                }).next("second")
                // 第二个事件名命为second next表示前面的事件和本事件要紧挨着  **严格连续性*
                .where(new SimpleCondition<UserBehaviorLogIn>() {
                    @Override
                    public boolean filter(UserBehaviorLogIn userBehaviorLogIn) throws Exception {
                        return userBehaviorLogIn.type.equals("fail");
                    }
                }).next("third")
                .where(new SimpleCondition<UserBehaviorLogIn>() {
                    @Override
                    public boolean filter(UserBehaviorLogIn userBehaviorLogIn) throws Exception {
                        return userBehaviorLogIn.type.equals("fail");
                    }
                }).within(Time.seconds(10));

        // 第一个参数是匹配的流，第二个参数是匹配的规则
        PatternStream<UserBehaviorLogIn> patterns = CEP.pattern(userKeyStream, pattern);
        DataStream<String> select = patterns.select(new PatternSelectFunction<UserBehaviorLogIn, String>() {
            @Override
            public String select(Map<String, List<UserBehaviorLogIn>> pattern) throws Exception {
                UserBehaviorLogIn first = pattern.get("first").iterator().next();
                UserBehaviorLogIn second = pattern.get("second").iterator().next();
                UserBehaviorLogIn third = pattern.get("third").iterator().next();
                String firstDays = DataUtils.getDays(first.loginTime);
                String secondDays = DataUtils.getDays(second.loginTime);
                String thirdDays = DataUtils.getDays(third.loginTime);
                String result = "用户： " + first.name + "  在三个时间段连续登录失败 ：  " + firstDays + " \t " + secondDays + "\t " + thirdDays;
                return result;
            }
        });

        select.print();

        env.execute("CEP-test");

    }

}
