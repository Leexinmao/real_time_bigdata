package com.realtime.flink.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 14:58
 **/
public class UserBehaviorLogIn {

    public String name;
    public String type;
    public String ipAddress;
    public Long loginTime;

    public UserBehaviorLogIn() {
    }

    public UserBehaviorLogIn(String name, String type, String ipAddress, Long loginTime) {
        this.name = name;
        this.type = type;
        this.ipAddress = ipAddress;
        this.loginTime = loginTime;
    }

    public UserBehaviorLogIn(String name, String type, Long loginTime) {
        this.name = name;
        this.type = type;
        this.loginTime = loginTime;
    }

    @Override
    public String toString() {
        return "UserBehaviorLogIn{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", loginTime=" + loginTime +
                '}';
    }
}
