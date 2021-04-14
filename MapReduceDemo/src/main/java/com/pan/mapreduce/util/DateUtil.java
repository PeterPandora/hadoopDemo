package com.pan.mapreduce.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 潘聪
 * @description
 * @date 2021/4/14 22:32
 */
public class DateUtil {
    public static String now(){
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(date);
    }
}
