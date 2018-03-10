package com.vowels.count;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class VowelsPerWord {
	
public static void main(String[] args) throws Exception{
	
	if (args.length != 2) {
	      System.err.println("Usage: wordcount <HDFSInputPath> <HDFSOutputPath>");
	      System.exit(2);
	    }		
		
		Configuration conf=new Configuration();
		Job job=new Job(conf,"vowelCount");
		
		job.setNumReduceTasks(0); 
		job.setJarByClass(VowelsPerWord.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(VowelsCountMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

public static class VowelsCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {

		private Text word = new Text();
	
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens())
			{
				String find = itr.nextToken().toLowerCase();
				System.out.println("the "+find);
				int vowelCount = 0;
				for(int i = 0; i<find.length();i++) {
					char ch = find.charAt(i);
					if(ch=='a' || ch=='e' || ch=='i' || ch=='o' || ch=='u') {
						++vowelCount;	
					}
				}
				System.out.println(find+" "+vowelCount);
				word.set(find);
				context.write(word,new IntWritable(vowelCount)); 
	
			}	
	}

}

}
