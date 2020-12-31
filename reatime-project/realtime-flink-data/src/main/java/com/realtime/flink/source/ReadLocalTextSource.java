package com.realtime.flink.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-25 17:15
 **/
public class ReadLocalTextSource {



    public static DataStream<String> readFileByLine(StreamExecutionEnvironment env,String path){

        return env.readTextFile(path);

    }

}
