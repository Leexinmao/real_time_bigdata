package com.realtime.flink.source;

import com.bigdata.flink.admin.model.Order;
import com.bigdata.flink.admin.util.GeneratorName;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-21 15:07
 **/
public class OrderSource extends RichParallelSourceFunction<Order> {

    private boolean running = true;

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {

//        public String NameId;
//        public String name;
//        public Integer age;

//        public Date createTime;
        Random r = new Random(Calendar.getInstance().getTimeInMillis());

        while (running){
            UUID uuid = UUID.randomUUID();
            String nameId = uuid.toString();
            String s = nameId.replaceAll("-", "");
            String substring = s.substring(0, 10);


            int nameNum = r.nextInt(3)+2;
            String name = GeneratorName.randomName(true,nameNum);

            int age = r.nextInt(10)+20;

            Date createTime = new Date();
            long time = createTime.getTime();
            Order order = new Order(substring, name, age, time);

            ctx.collect(order);

        }
        Thread.sleep(100);

    }

    @Override
    public void cancel() {
        running = false;
    }
}
