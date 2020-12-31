package com.realtime.flink.sink;

import com.bigdata.flink.admin.model.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-17 12:25
 **/
public class RichSinkMysqlFunction extends RichSinkFunction<SensorReading> {

    private Connection conn;
    private PreparedStatement insertStmt;
    private PreparedStatement updateStmt;

    @Override
    public void open(Configuration parameters) throws Exception {

        conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/sensor",
                "root",
                "123456"
        );
        insertStmt = conn.prepareStatement("INSERT INTO temps (id, temp) VALUES (?, ?)");
        updateStmt = conn.prepareStatement("UPDATE temps SET temp = ? WHERE id = ?");
    }
    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        updateStmt.setDouble(1, value.getTemperature());
        updateStmt.setString(2, value.getId());
        updateStmt.execute();

        if (updateStmt.getUpdateCount() == 0) {
            insertStmt.setString(1, value.getId());
            insertStmt.setDouble(2, value.getTemperature());
            insertStmt.execute();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        insertStmt.close();
        updateStmt.close();
        conn.close();
    }

}
