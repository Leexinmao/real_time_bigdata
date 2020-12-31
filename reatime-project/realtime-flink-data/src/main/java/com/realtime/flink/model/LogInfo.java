package com.realtime.flink.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-27 17:29
 **/
public class LogInfo {

//    ip	String	访问的IP
//    userId	Long	访问的userId
//    eventTime	Long	访问时间
//    method	String	访问方法 GET/POST/PUT/DELETE
//    url	String	访问的url

    public String ip;

    public Long userId;

    public Long eventTime;

    public String method;

    public String url;

    public LogInfo(String ip, Long userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    public LogInfo() {
    }
}
