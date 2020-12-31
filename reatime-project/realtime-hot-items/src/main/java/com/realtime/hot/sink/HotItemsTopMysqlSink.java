package com.realtime.hot.sink;

import com.alibaba.fastjson.JSON;

import com.realtime.hot.model.GoodsTopItems;
import com.realtime.hot.utils.DateTimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-29 17:33
 **/
public class HotItemsTopMysqlSink extends RichSinkFunction<String> {


    private Connection conn;
    private PreparedStatement insertStmt;
    private PreparedStatement updateStmt;

    @Override
    public void open(Configuration parameters) throws Exception {

        conn = DriverManager.getConnection(
                "jdbc:mysql://192.168.2.200:3306/t_big_hot?allowMultiQueries=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false",
                "root",
                "123456"
        );
        insertStmt = conn.prepareStatement("INSERT INTO t_big_hot_items_top(t_item_id,t_top_count,t_top_num,t_settle_time) VALUES (?, ?, ?, ?)");

    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        GoodsTopItems goodsTopItems = JSON.parseObject(value, GoodsTopItems.class);
        insertStmt.setLong(1, goodsTopItems.getItemId());
        insertStmt.setLong(2, goodsTopItems.getCount());
        insertStmt.setLong(3, goodsTopItems.getTopNum());
        insertStmt.setString(4, DateTimeUtils.stampToDataTime(goodsTopItems.getWindowEnd()));
        insertStmt.execute();

    }

}
