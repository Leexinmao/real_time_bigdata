package com.realtime.hot.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-29 16:16
 **/
public class GoodsTopItems {

    private Long itemId; // 商品id

    private Integer topNum; //排名

    private Long count;  // 点击量

    private Long windowEnd; // 时间戳

    public GoodsTopItems() {
    }

    public GoodsTopItems(Long itemId, Long count, Long windowEnd, Integer topNum) {
        this.itemId = itemId;
        this.topNum = topNum;
        this.count = count;
        this.windowEnd = windowEnd;
    }

    public Long getItemId() {
        return itemId;
    }

    public GoodsTopItems setItemId(Long itemId) {
        this.itemId = itemId;
        return this;
    }

    public Integer getTopNum() {
        return topNum;
    }

    public GoodsTopItems setTopNum(Integer topNum) {
        this.topNum = topNum;
        return this;
    }

    public Long getCount() {
        return count;
    }

    public GoodsTopItems setCount(Long count) {
        this.count = count;
        return this;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public GoodsTopItems setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
        return this;
    }

    @Override
    public String toString() {
        return "GoodsTopItems{" +
                "itemId=" + itemId +
                ", topNum=" + topNum +
                ", count=" + count +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
