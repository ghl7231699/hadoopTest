package com.bigdata.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMapReduce extends Configured implements Tool {
	/*
	 * input->map->shuffle->reduce->output
	 */

	//mapper,输入数据变成键值对，一行转化为一条
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable();

		protected void map(LongWritable key, Text value, Reducer.Context context)
				throws IOException, InterruptedException {

			//每一行数据变成键值对
			String lineValue = value.toString();
			String[] strs = lineValue.split(" ");
			//每一行的键值对进行统计
			for(String str:strs) {
				mapOutputKey.set(str);
				mapOutputValue.set(1);
				context.write(mapOutputKey, mapOutputValue);
			}
		}
	}
	//reducer,map的输出就是我们reduce的输入
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable outputValue = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
							  Context context) throws IOException, InterruptedException {
			//汇总工作
			int sum=0;
			for(IntWritable value:values) {
				sum+=value.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);

		}
	}
	//driver:任务相关设置
	public int run(String[] args) throws Exception{
		//获取相关配置
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(WordCountMapReduce.class);
		//设置input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		//设置output
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		//设置map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//shuffle优化
		//job.setPartitionerClass(cls);
		//job.setSortComparatorClass(cls);
		//job.setCombinerClass(cls);
		//job.setGroupingComparatorClass(cls);

		//设置reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//将job提交给Yarn
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess?0:1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		//保存的输入输出路径
//		args = new String[]{
//				"hdfs://ghl01:50070/user/root/ghl/mapreduce/input",
//				"hdfs://ghl01:50070/user/root/ghl/mapreduce/outpu"
//		};
//		/Users/guhongliang/IdeaProjects/hadoopTest

//		args = new String[]{
//				"hdfs://ghl01:50070/user/root/ghl/mapreduce/input",
//				"hdfs://ghl01:50070/user/root/ghl/mapreduce/outpu"
//		};
		//将任务跑起来
		//int statas = new WordCountMapReduce().run(args);
		int statas = ToolRunner.run(conf, new WordCountMapReduce(), args);
		//关闭我们的job
		System.exit(statas);
	}

}