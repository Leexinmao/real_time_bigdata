package com.realtime.common.custom;

import com.realtime.common.utils.OperationFileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

public class ReadFileToTable {


    public static void main(String[] args) {

        int i = maxPrimeNumber();
        System.out.println(i);
        testPri(i);

//        String path = "D:\\bigdate\\gitproject\\real_time_bigdata\\reatime-project\\realtime-common\\src\\main\\resource\\useruv.txt";
//
//        String s = UserUvStringInsert(path);
//        System.out.println(s);


    }

    public static String UserUvStringInsert(String path) {

        StringBuffer buffer = new StringBuffer("insert into `t_uv`(active_time,user_id,age) VALUES");
        List<String> list = OperationFileUtils.readTextByFile(path);
        list.forEach(r -> {
            String[] split = r.split(",");
            String activeTime = split[0];
            String userId = split[1];
            String age = split[2];
            buffer.append("('2020-01-").append(activeTime).append("','")
                    .append(userId).append("',")
                    .append(age).append("),")
                    .append("\n");
        });

        return buffer.toString();

    }


    public static int maxPrimeNumber(){
        int maxNum = 400;
        int minNum = 200;
        int primeNumber=0;
        boolean flage = false;
        for(int i=maxNum;i>minNum;i--){
            System.out.println(i);

            for(int j=i/2;j>1;j--){
                if(i%j == 0){
                    flage = true;
                }
                if (flage){
                    flage = false;
                    break;
                }
                if (j==2 && flage == false){
                    return i;
                }
            }
        }
        return primeNumber;

    }

    public static void testPri(int num){

        for (int i = num/2; i>1 ; i--) {
            if (num%i==0){
                System.out.println(num+"  这个数不是素数");
            }

        }


    }

}
