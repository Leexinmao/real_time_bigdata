package com.realtime.hot.calc;
import com.alibaba.fastjson.JSON;
import com.realtime.hot.goods.model.GoodsClick;
import com.realtime.hot.goods.sink.KafkaSink;
import com.realtime.hot.goods.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description: 读取用户点击商品，传给kafka
 * @author: lxm
 * @create: 2020-12-26 15:12
 **/
public class ReadHotGoods {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        String path = "D:\\bigdata\\realtimeproject\\realtime-anlog\\realtime_hot_goods\\src\\main\\resources\\UserBehavior.csv";
        DataStreamSource<String> streamSource = env.readTextFile(path);
        DataStream<String> mapStream = streamSource.map(r -> {
            String[] str = r.split(",");
//            Thread.sleep(50);
            GoodsClick goodsClick = new GoodsClick(Long.parseLong(str[0]), Long.parseLong(str[1]), Integer.parseInt(str[2]), str[3], Long.parseLong(str[4]) * 1000L);
            String jsonString = JSON.toJSONString(goodsClick);
            return jsonString;
        });

        KafkaSink.KafkaSource(mapStream, "hadoop110:9092", "hots");

        mapStream.print();


        env.execute("ReadHotGoods");

    }


}
