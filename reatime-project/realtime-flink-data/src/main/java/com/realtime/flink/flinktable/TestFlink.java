package com.realtime.flink.flinktable;


import java.io.*;

/**
 * @description: description
 * @author: lxm
 * @create: 2020-12-21 15:47
 **/
public class TestFlink {

    public static void main(String[] args) throws Exception {

        String conent = "hellowork";

        File file = new File("D:\\bigdata\\file\\iowrite.txt");
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write(conent + "\r\n");
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

}
