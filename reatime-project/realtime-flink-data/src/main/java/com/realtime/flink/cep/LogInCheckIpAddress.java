package com.realtime.flink.cep;


import com.realtime.flink.model.UserBehaviorLogIn;
import com.realtime.flink.model.UserBehaviorSource;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: 检测同一个账户在不同ip连续登录5次
 * @author: lxm
 * @create: 2020-12-22 17:15
 **/
public class LogInCheckIpAddress {

    private static String  IP_ADDRESS = "0.0.0.0";

    public static void main(String[] args) throws Exception {

        Map<String,String> ipMap = new HashMap<>();
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


        Pattern<UserBehaviorLogIn, UserBehaviorLogIn> patternResult = Pattern.<UserBehaviorLogIn>begin("first")
                .where(new SimpleCondition<UserBehaviorLogIn>() {
                    @Override
                    public boolean filter(UserBehaviorLogIn userBehaviorLogIn) throws Exception {


                        String oldIp = ipMap.get(userBehaviorLogIn.name);
                        if (ipMap.get(userBehaviorLogIn.name) == null){
                            ipMap.put(userBehaviorLogIn.name,userBehaviorLogIn.ipAddress);
                            System.out.println("用户 =  "+userBehaviorLogIn.name+"  第一次的ip为：  =  "+ipMap.get(userBehaviorLogIn.name));
                            return false;
                        }

                        if (oldIp.equals(userBehaviorLogIn.ipAddress)){
                            return false;
                        }else {
                            ipMap.put(userBehaviorLogIn.name,userBehaviorLogIn.ipAddress);
                            System.out.println("用户 =  "+userBehaviorLogIn.name+ "修改之前的ip为   =  "+oldIp+ "\\t\\t修改之后的ip为：  =  "+ipMap.get(userBehaviorLogIn.name));
                            return true;
                        }
                    }
                }).times(5)
                .within(Time.seconds(5));


        PatternStream<UserBehaviorLogIn> pattern = CEP.pattern(userKeyStream, patternResult);

        DataStream<String> first = pattern.select(new PatternSelectFunction<UserBehaviorLogIn, String>() {
            @Override
            public String select(Map<String, List<UserBehaviorLogIn>> pattern) throws Exception {
                List<UserBehaviorLogIn> first = pattern.get("first");
                String name = first.get(0).name;
                StringBuffer buffer = new StringBuffer("用户：  ");
                buffer.append(name).append(" 在这几次连续登录在不同的电脑 ： \t");
                first.forEach(r -> {
                    buffer.append(r.ipAddress).append("\t\t\t");
                });

                return buffer.toString();
            }
        });

        first.print();

        env.execute("LogInCheckIpCep");
    }

}
