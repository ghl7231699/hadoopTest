package com.bigdata.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMapReduce2 extends Configured implements Tool{
	/*
	 * input->map->shuffle->reduce->output
	 */

	//mapper,输入数据变成键值对，一行转化为一条
	public static class WordCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
				
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			IntWritable intWritable = new IntWritable(Integer.parseInt(value.toString()));
            context.write(intWritable, intWritable);
		}
	}
	//reducer,map的输出就是我们reduce的输入
	public static class WordCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable>{
		
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			for(IntWritable value : values) {
				context.write(value, NullWritable.get());
			}	
			
		}
	}
	
    public static class IteblogPartitioner extends Partitioner<IntWritable, IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int keyInt = Integer.parseInt(key.toString());
            if (keyInt < 10000) {
                return 0;
            } else if (keyInt < 20000) {
                return 1;
            } else {
                return 2;
            }
        }
    }
    
	//driver:任务相关设置
	public int run(String[] args) throws Exception{
		//获取相关配置
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(WordCountMapReduce2.class);
		//设置input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		//设置output
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		//设置map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//shuffle优化
		job.setPartitionerClass(IteblogPartitioner.class);
		//job.setSortComparatorClass(cls);
		//job.setCombinerClass(cls);
		//job.setGroupingComparatorClass(cls);
		
		//设置reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		//将job提交给Yarn
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//保存的输入输出路径
		args = new String[]{
			"hdfs://lee01.cniao5.com:8020/user/root/mapreduce/input",
			"hdfs://lee01.cniao5.com:8020/user/root/mapreduce/output2"
		};
		//将任务跑起来
		//int statas = new WordCountMapReduce().run(args);
		int statas = ToolRunner.run(conf, new WordCountMapReduce2(), args);
		//关闭我们的job
		System.exit(statas);
	}

}
