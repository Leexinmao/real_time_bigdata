package com.realtime.flink.model;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-11-06 18:42
 **/
public class SensorReading {

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    private String id;

    private Long timestamp;

    private Double temperature;

    public String getId() {
        return id;
    }

    public SensorReading setId(String id) {
        this.id = id;
        return this;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public SensorReading setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Double getTemperature() {
        return temperature;
    }

    public SensorReading setTemperature(Double temperature) {
        this.temperature = temperature;
        return this;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
