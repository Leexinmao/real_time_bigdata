package com.realtime.flink.map.model;

public class WordsCount {

    private String name;

    private Long num;

    public WordsCount(String name, Long num) {
        this.name = name;
        this.num = num;
    }

    public WordsCount() {
    }

    @Override
    public String toString() {
        return "WordsCount{" +
                "name='" + name + '\'' +
                ", num=" + num +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getNum() {
        return num;
    }

    public void setNum(Long num) {
        this.num = num;
    }
}
