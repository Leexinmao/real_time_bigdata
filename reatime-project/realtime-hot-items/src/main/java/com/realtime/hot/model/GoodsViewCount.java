package com.realtime.hot.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-29 14:34
 **/
public class GoodsViewCount {

    private Long itemId;

    private Long windowEnd;

    private Long count;

    public GoodsViewCount() {
    }

    public GoodsViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public GoodsViewCount setItemId(Long itemId) {
        this.itemId = itemId;
        return this;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public GoodsViewCount setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
        return this;
    }

    public Long getCount() {
        return count;
    }

    public GoodsViewCount setCount(Long count) {
        this.count = count;
        return this;
    }
}
