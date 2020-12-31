package com.realtime.hot.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-26 15:15
 **/
public class GoodsClick {

    public Long userId;

    public Long itemId;

    public Integer categoryId;

    public String behavior;

    public Long timestamp;

    public GoodsClick() {
    }

    public GoodsClick(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "GoodsClick{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
