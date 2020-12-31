package com.realtime.flink.window;

import com.bigdata.flink.admin.model.SensorReading;
import com.bigdata.flink.admin.source.SensouSoure;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.collection.Iterator;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;

/**
 * @description: 煮给你抬变量
 * @author: lxm
 * @create: 2020-11-24 23:23
 **/
public class ListStateFunctionFlink {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);


        DataStream<SensorReading> stream = env.addSource(new SensouSoure());

        stream.filter(r -> r.getId().equals("sensor_1"))
                .keyBy(t -> t.getId())
                .process(new MyListStateFun())
                .print();

        env.execute();

    }

    public static class MyListStateFun extends KeyedProcessFunction<String,SensorReading,String>{

        ListState<SensorReading> listState = null;


        ValueState<Long> timer = null;

        @Override
        public void open(Configuration parameters) throws Exception {


            // 初始化数据要在这里进行
             listState = getRuntimeContext().getListState(new ListStateDescriptor<SensorReading>("list-state", SensorReading.class ));

             timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-state", Long.class ));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            listState.add(value);
            if (timer.value() == null || timer.value() == 0L){
                Long tx = ctx.timerService().currentProcessingTime() +10* 1000L;
                ctx.timerService().registerProcessingTimeTimer(tx );   // 注册一个定时器 onTimer方法中执行
                timer.update(tx);

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            // 不能直接对列表状态值进行计算

            ListBuffer<SensorReading> listBuffer = new ListBuffer<>();
            Iterable<SensorReading> sensorReadings = listState.get();
            for (SensorReading reading : listState.get()){
                listBuffer.$plus$eq(reading);
            }
            List<SensorReading> sensorReadingList = listBuffer.toList();


            Iterator<SensorReading> iterator = sensorReadingList.iterator();
            while (iterator.hasNext()){
                SensorReading next = iterator.next();
                System.out.println(next.toString());
            }




            out.collect("当前状态里总共有   "  +listBuffer.size() + "  个状态值！！ ");
            listBuffer.clear();
            listState.clear();
            timer.clear();

        }
    }

}










