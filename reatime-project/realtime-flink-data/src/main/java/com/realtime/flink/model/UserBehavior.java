package com.realtime.flink.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-27 17:01
 **/
public class UserBehavior {

    public Long userId;

    public Long itemId;

    public Integer categoryId;

    public String behavior;

    public Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
