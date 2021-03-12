package com.realtime.common.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class OperationFileUtils {


    public static List<String> readTextByFile(String path) {
        File file = new File(path);
        List<String> list = new ArrayList<>();
        if (file.isFile() && file.exists()) {
            try {
                FileInputStream fileInputStream = new FileInputStream(file);
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String text = null;
                while ((text = bufferedReader.readLine()) != null) {
                    list.add(text);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }


}
