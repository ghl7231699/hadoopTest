package com.bigdata.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

public class SortMapReduce extends Configured implements Tool{
	/*
	 * input->map->shuffle->reduce->output
	 */

	//mapper,输入数据变成键值对，一行转化为一条
	public static class SortMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable>{
		
		private PairWritable mapOutputKey = new PairWritable();
		private IntWritable mapOutputValue = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			//每一行数据变成键值对
			String lineValue = value.toString();
			String[] strs = lineValue.split(" ");
			
			//验证数据是否合法
			if (2 != strs.length) {
				return;
			}
			
			mapOutputKey.set(strs[0], Integer.valueOf(strs[1]));
			mapOutputValue.set(Integer.valueOf(strs[1]));

			context.write(mapOutputKey, mapOutputValue);
		}
	}
	//reducer,map的输出就是我们reduce的输入
	public static class SortReducer extends Reducer<PairWritable, IntWritable, Text, IntWritable>{
		
		private Text outputKey = new Text();
		
		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
//			//用一个list，将key相同的value的值存储起来
//			List<IntWritable> valueList = new ArrayList<IntWritable>(); 
//			//迭代之后将值放进去
//			for(IntWritable value:values) {
//				valueList.add(value);
//			}
//			Collections.sort(valueList);//资源开销过大
//			for(Integer value : valuesList) {
//				 context.write(key, new IntWritable(value));
//	        }
			
			for (IntWritable value : values) {

				// set output key
				outputKey.set(key.getFirst());

				context.write(outputKey, value);
			}
			
			
		}
	}
	//driver:任务相关设置
	public int run(String[] args) throws Exception{
		//获取相关配置
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(SortMapReduce.class);
		//设置input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		//设置output
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		//设置map
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//shuffle优化
		job.setPartitionerClass(FirstPartitioner.class);
		//job.setSortComparatorClass(cls);
		//job.setCombinerClass(cls);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		
		//设置reduce
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//将job提交给Yarn
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//保存的输入输出路径
		args = new String[]{
			"hdfs://lee01.cniao5.com:8020/user/root/mapreduce/sort/input",
			"hdfs://lee01.cniao5.com:8020/user/root/mapreduce/sort/output"
		};
		//将任务跑起来
		//int statas = new WordCountMapReduce().run(args);
		int statas = ToolRunner.run(conf, new SortMapReduce(), args);
		//关闭我们的job
		System.exit(statas);
	}

}
