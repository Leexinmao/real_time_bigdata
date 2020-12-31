/**   
* @Title: DateStyle.java 
* @Package com.xingyun.xyb2b.common.util 
* @Description: TODO(用一句话描述该文件做什么) 
* @author bond
* @date 2016年8月30日 下午4:34:31 
* @company 版权所有 深圳市天行云供应链有限公司
* @version V1.0   
*/
package com.realtime.flink.utils;

/** 
* @ClassName: DateStyle 
* @Description: TODO(这里用一句话描述这个类的作用) 
* @author bond
* @date 2016年8月30日 下午4:34:31 
*  
*/
public enum DateStyle {
    MM_DD("MM-dd"),
    YYYY_MM("yyyy-MM"),  
    YYYY_MM_DD("yyyy-MM-dd"),  
    MM_DD_HH_MM("MM-dd HH:mm"),  
    MM_DD_HH_MM_SS("MM-dd HH:mm:ss"),  
    YYYY_MM_DD_HH_MM("yyyy-MM-dd HH:mm"),  
    YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),  
    YYYYMMDDHHMMSS("yyyyMMddHHmmss"), 
    MM_DD_EN("MM/dd"),  
    YYYY_MM_EN("yyyy/MM"),  
    YYYY_MM_DD_EN("yyyy/MM/dd"),  
    MM_DD_HH_MM_EN("MM/dd HH:mm"),  
    MM_DD_HH_MM_SS_EN("MM/dd HH:mm:ss"),  
    YYYY_MM_DD_HH_MM_EN("yyyy/MM/dd HH:mm"),  
    YYYY_MM_DD_HH_MM_SS_EN("yyyy/MM/dd HH:mm:ss"),
    YYYYMMDD("yyyyMMdd"),
    MM_DD_CN("MM月dd日"),  
    YYYY_MM_CN("yyyy年MM月"),  
    YYYY_MM_DD_CN("yyyy年MM月dd日"),  
    MM_DD_HH_MM_CN("MM月dd日 HH:mm"),  
    MM_DD_HH_MM_SS_CN("MM月dd日 HH:mm:ss"),  
    YYYY_MM_DD_HH_MM_CN("yyyy年MM月dd日 HH:mm"),  
    YYYY_MM_DD_HH_MM_SS_CN("yyyy年MM月dd日 HH:mm:ss"),  
      
    HH_MM("HH:mm"),  
    HH_MM_SS("HH:mm:ss");  
      
      
    private String value;  
      
    DateStyle(String value) {  
        this.value = value;  
    }  
      
    public String getValue() {  
        return value;  
    }  
}
