package com.words.line;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordsPerLineDriver {
	
	public static class WordsPerLineMapper extends Mapper<LongWritable,Text, Text, IntWritable>
	{
		int lineno = 1;
		public void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException
		{
			StringTokenizer token = new StringTokenizer(value.toString());
            while(token.hasMoreTokens())
            {
            	String word = token.nextToken();
            	String l = "Line"+lineno;   
            	Text outputKey = new Text(l);
            	IntWritable outputValue = new IntWritable(1);
            	con.write(outputKey, outputValue);
            	
            }
            lineno++;
          }
	 }
	
	public static class WordsPerLineReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value: values)
				sum+=value.get();
			context.write(key , new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception{
		
		if (args.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		    }
		
		Configuration conf=new Configuration();
		Job job=new Job(conf,"WordsPerLine");
		
		job.setJarByClass(WordsPerLineDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordsPerLineMapper.class);
		job.setReducerClass(WordsPerLineReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
