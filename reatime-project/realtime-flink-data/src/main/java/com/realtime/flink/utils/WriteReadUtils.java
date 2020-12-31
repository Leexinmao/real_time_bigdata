package com.realtime.flink.utils;

import java.io.*;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-22 11:44
 **/
public class WriteReadUtils {

    public static void writeToFile(String content,String path){

        File file = new File(path);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write(content + "\r\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void writeToFileToIoWrite(String content){
        String path = "D:\\bigdata\\file\\iowrite.txt";
        writeToFile(content,path);
    }

}
