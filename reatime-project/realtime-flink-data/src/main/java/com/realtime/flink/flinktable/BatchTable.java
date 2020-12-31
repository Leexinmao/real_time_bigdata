package com.realtime.flink.flinktable;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-26 20:29
 **/
public class BatchTable {

    public static void main(String[] args) {

        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
    }


}
