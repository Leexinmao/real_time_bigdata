package com.realtime.flink.source;


import com.realtime.flink.model.UserBehaviorLogIn;
import com.realtime.flink.utils.WriteReadUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 14:59
 **/
public class UserBehaviorSource extends RichParallelSourceFunction<UserBehaviorLogIn> {

    private boolean running = true;
    private String type;


    @Override
    public void run(SourceContext<UserBehaviorLogIn> ctx) throws Exception {


        while (running){
            Random r = new Random(Calendar.getInstance().getTimeInMillis()+88888);
            int nameNum = r.nextInt(10000) + 10000;
            String name = listName();

            if (nameNum % 2 == 0) {
                type = "success";
            } else {
                type = "fail";
            }
            long times = System.currentTimeMillis();
            String ipAddress = listIp();

            UserBehaviorLogIn userBehaviorLogIn = new UserBehaviorLogIn(name, type, ipAddress,times);

            WriteReadUtils.writeToFile(userBehaviorLogIn.toString(),"D:\\bigdata\\file\\temp.txt");
            ctx.collect(userBehaviorLogIn);

            Thread.sleep(200);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }


    public static String listName(){

        List<String> list = new ArrayList<>();
        list.add("lisa");
        list.add("lili");
        list.add("alex");
        list.add("frade");
        list.add("tomos");
        list.add("breals");
        list.add("bill");

        Random random = new Random(System.currentTimeMillis());
        int i = random.nextInt(3) + 4;
        return list.get(i);
    }

    public static String listIp(){

        List<String> list = new ArrayList<>();
        list.add("127.1.2.8");
        list.add("127.1.2.9");
        list.add("127.1.2.10");
        list.add("127.1.2.11");
        list.add("127.1.2.12");
        list.add("127.1.2.13");
        list.add("127.1.2.15");
        list.add("127.1.2.16");
        list.add("127.1.4.33");
        list.add("127.1.2.56");
        list.add("127.1.2.87");
        list.add("127.1.2.77");
        list.add("127.1.2.44");
        list.add("127.1.2.41");

        Random random = new Random(System.currentTimeMillis() - 156455);
        int i = random.nextInt(13);
        return list.get(i);
    }
}
