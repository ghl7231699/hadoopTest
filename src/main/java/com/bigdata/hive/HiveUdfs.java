package com.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

public class HiveUdfs extends UDF {
    public String evaluate(IntWritable in) {
        String txt;
        //对学生成绩进行评级
        if (in == null) {
            return "没有成绩";
        }
        if (in.get() < 180) {
            txt = "差";
        } else if (in.get() < 240) {
            txt = "良";
        } else {
            txt = "优";
        }
        return txt;
    }
}
