package com.realtime.flink.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-07 20:54
 **/
public class WordCount {

    public String word;
    public int count;

    public WordCount() {
    }

    public WordCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public WordCount setWord(String word) {
        this.word = word;
        return this;
    }

    public int getCount() {
        return count;
    }

    public WordCount setCount(int count) {
        this.count = count;
        return this;
    }
}
