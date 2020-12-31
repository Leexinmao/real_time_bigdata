package com.realtime.bigdata.data.controller;




import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.realtime.bigdata.data.common.GmallConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String>   kafkaTemplate;

    private final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log) {
        JSONObject logObj = JSON.parseObject(log);
        // 添加时间戳
        logObj = addTS(logObj);
        // 日志落盘
        saveLog(logObj);
        // 发送到 kafka
        sendToKafka(logObj);
        return "success";

    }

    @GetMapping("/test")
    public ResponseEntity<String> test(@RequestParam("test") String s) {

        String s1 = " hello";
        System.out.println("ajdlfskkkkkkkkkkkkk");
        logger.info(s1 +s);

        return ResponseEntity.ok(s1 + s);
    }

    /**
     * 添加时间戳
     *
     * @param logObj
     * @return
     */
    public JSONObject addTS(JSONObject logObj) {
        logObj.put("ts", System.currentTimeMillis());
        return logObj;
    }

    public void saveLog(JSONObject logObj) {
        logger.info(logObj.toJSONString());
    }


    private void sendToKafka(JSONObject logObj) {

        String logType = logObj.getString("logType");
        String topicName = GmallConstant.TOPIC_STARTUP;
        if ("event".equals(logType)) {
            topicName = GmallConstant.TOPIC_EVENT;
        }
        kafkaTemplate.send(topicName, logObj.toJSONString());
    }


}