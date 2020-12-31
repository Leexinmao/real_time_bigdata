package com.realtime.flink.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-27 17:22
 **/
public class ItemViewCount {

    private Long itemId;

    private Long windowEnd;

    private Long count;

    public ItemViewCount() {
    }


    public Long getItemId() {
        return itemId;
    }

    public ItemViewCount setItemId(Long itemId) {
        this.itemId = itemId;
        return this;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public ItemViewCount setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
        return this;
    }

    public Long getCount() {
        return count;
    }

    public ItemViewCount setCount(Long count) {
        this.count = count;
        return this;
    }

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
