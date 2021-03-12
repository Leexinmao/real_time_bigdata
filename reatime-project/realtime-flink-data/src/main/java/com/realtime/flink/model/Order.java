package com.realtime.flink.model;

public class Order {

    public String nameId;
    public String name;
    public int age;
    public long time;
    public long rowtime;





    public Order(String nameId, String name, int age, long time) {
        this.nameId = nameId;
        this.name = name;
        this.age = age;
        this.time = time;
    }


    public Order() {
    }
}
