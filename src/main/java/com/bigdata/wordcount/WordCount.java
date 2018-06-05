package com.bigdata.wordcount;

import com.bigdata.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static final String INPUT_PATH = "hdfs://ghl02:8020/user/ghl/mapreduce/input/test.input";
    public static final String OUTPUT_PATH = "hdfs://ghl02:8020/user/ghl/mapreduce/output";


    //1.Map class
    //参数：偏移量+内容->内容+计数
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();
        private IntWritable mapOutputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //将读取的文件变成：偏移量+内容
            String linevalue = value.toString();
            //System.out.println("linevalue----"+linevalue);
            //根据" "去切分我们的单词,并处理
            String[] strs = linevalue.split(" ");
            for (String str : strs) {
                //key:单词 value：1
                mapOutputKey.set(str);
                mapOutputValue.set(1);
                context.write(mapOutputKey, mapOutputValue);
                //System.out.println("<"+mapOutputKey+","+mapOutputValue+">");
            }
            //将结果传递出去
        }
    }

    //2.Reducer class
    //参数：内容+计数->内容+计数
    public static class WordCountRecucer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            //汇总
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
                //System.out.println(value.get()+" ");
            }
            outputValue.set(sum);
            context.write(key, outputValue);
        }
    }

    //3.job class
    public int run(String[] args) throws Exception {

        //获取我们的配置
        Configuration conf = new Configuration();

        //检测本地是否存在输出文件夹
        HdfsUtils.deleteFile(conf, OUTPUT_PATH);

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        //设置input与output
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        //设置map与reduce
        //需要设置的内容 类+输出key与value
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(WordCountRecucer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean isSuccess = job.waitForCompletion(true);

        if (job.waitForCompletion(true)) {
            HdfsUtils.cat(conf, args[1] + "/part-r-00000");
            System.out.println("success");
        } else {
            System.out.println("fail");
        }

        return isSuccess ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        //参数
        args = new String[]{INPUT_PATH, OUTPUT_PATH};
        //跑我们的任务
        int status = new WordCount().run(args);

        System.exit(status);
    }
}
