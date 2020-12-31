package com.realtime.flink.cep;


import com.realtime.flink.model.UserBehaviorLogIn;
import com.realtime.flink.model.UserBehaviorSource;
import com.realtime.flink.utils.DataUtils;
import com.realtime.flink.utils.WriteReadUtils;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @description: 检测在5s内用户连续登录失败5次
 * @author: lxm
 * @create: 2020-12-22 15:49
 **/
public class LogInCheckFailCep {

    private final static Integer LOGIN_FAIL_NUM = 5;

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
        userKeyStream.process(new MyKeyFuncation1());
        Pattern<UserBehaviorLogIn, UserBehaviorLogIn> patternResult = Pattern.<UserBehaviorLogIn>begin("first")
                .where(new SimpleCondition<UserBehaviorLogIn>() {
                    @Override
                    public boolean filter(UserBehaviorLogIn userBehaviorLogIn) throws Exception {
                        return userBehaviorLogIn.type.equals("fail");
                    }
                }).times(LOGIN_FAIL_NUM)
                .within(Time.seconds(5));
        PatternStream<UserBehaviorLogIn> pattern = CEP.pattern(userKeyStream, patternResult);
        DataStream<String> first = pattern.select(new PatternSelectFunction<UserBehaviorLogIn, String>() {
            @Override
            public String select(Map<String, List<UserBehaviorLogIn>> pattern) throws Exception {
                List<UserBehaviorLogIn> first = pattern.get("first");
                String name = first.get(0).name;
                StringBuffer buffer = new StringBuffer("用户：  ");
                buffer.append(name).append(" 在这几个个时间段连续登录失败 ： \t");
                first.forEach(r -> {
                    buffer.append(DataUtils.getDays(r.loginTime)).append("\t\t\t");
                });

                return buffer.toString();
            }
        });

        first.print();

        env.execute("LogInCheckFailCep");

    }


    public static class MyKeyFuncation1 extends KeyedProcessFunction<String, UserBehaviorLogIn, UserBehaviorLogIn> {


        @Override
        public void processElement(UserBehaviorLogIn value, Context ctx, Collector<UserBehaviorLogIn> out) throws Exception {

            String s = value.toString();
            WriteReadUtils.writeToFileToIoWrite(s);
            out.collect(value);
        }
    }

}
