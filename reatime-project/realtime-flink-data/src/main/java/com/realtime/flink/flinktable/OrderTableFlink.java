package com.realtime.flink.flinktable;

import com.bigdata.flink.admin.model.Order;
import com.bigdata.flink.admin.source.OrderSource;
import com.bigdata.flink.admin.util.DataUtils;
import com.bigdata.flink.admin.util.WriteReadUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-21 15:25
 **/
public class OrderTableFlink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);
        DataStreamSource<Order> source = env.addSource(new OrderSource());


        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                    @Override
                    public long extractTimestamp(Order order, long l) {
                        return order.rowtime;
                    }
                }));
        tableEnv.createTemporaryView("orders",orderSingleOutputStreamOperator,$("nameId"),$("name"),$("age"),$("rowtime").rowtime().as("ts") );

//        Table tumble = tableEnv.sqlQuery("select nameId,count(nameId), sum(age),TUMBLE_START(ts, INTERVAL '1' SECOND)," +
//                "TUMBLE_END(ts, INTERVAL '1' SECOND)" +
//                " from orders group by TUMBLE(ts, INTERVAL '1' SECOND), nameId"); // 滑动窗口
        Table tumble = tableEnv.sqlQuery("select nameId,name,max(age),max(ts),TUMBLE_START(ts, INTERVAL '5' SECOND), TUMBLE_END(ts, INTERVAL '5' SECOND) from orders group by TUMBLE(ts, INTERVAL '5' SECOND), nameId,name");
        DataStream<Row> tuple2DataStream = tableEnv.toAppendStream(tumble, Row.class);
        DataStream<Order> process = tuple2DataStream.process(new ProcessMyFunction());
        process.print();

//        tuple2DataStream.print();
        env.execute("OrderTableFlink");
    }


    public static class  ProcessMyFunction extends ProcessFunction<Row,Order>{

        @Override
        public void processElement(Row value, Context ctx, Collector<Order> out) throws Exception {
//            , String name, , Long rowtime

            Object field1 = value.getField(0);
            Object field2 = value.getField(1);
            Object field3 = value.getField(2);
            Object field4 = value.getField(3);


            String nameId =field1.toString();
            String name =  field2.toString();
            Integer age = Integer.parseInt(field3.toString());
            String rowTime = DataUtils.formatGM8(field4.toString());
            Order order = new Order(nameId,name,age,rowTime);
            WriteReadUtils.writeToFileToIoWrite(order.toString());
            out.collect(order);

        }
    }

}
