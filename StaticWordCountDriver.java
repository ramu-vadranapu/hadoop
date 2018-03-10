package com.kelly.hadoop1;

import java.io.IOException;
import java.util.StringTokenizer;

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


public class StaticWordCountDriver {
	
	public static class StaticWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens())
			{
				String find = itr.nextToken();
				if(find.equals("hadoop") ) {
				word.set(find.toString());
				context.write(word,one); 
				}
			}
		}
		
	}
	
	public static class StaticWOrdCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable val:values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"staticWordCount");

		job.setJarByClass(StaticWordCountDriver.class);
		job.setMapperClass(StaticWordCountMapper.class);
		job.setReducerClass(StaticWOrdCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);	

	}
}
