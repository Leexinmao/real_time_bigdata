package com.realtime.flink.flinktable;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 18:22
 **/
public class MysqlConnnector {

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String name            = "tb_brand";
        String defaultDatabase = "leyu";
        String username        = "root";
        String password        = "123456";
        String baseUrl         = "jdbc:mysql://localhost:3306/leyu";

//        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
//
//        tableEnv.registerCatalog("tb_brand", catalog);



    }
}
