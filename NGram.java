package edu.cmu;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGram {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private IntWritable times = new IntWritable();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().trim().toLowerCase();
			line = line.replaceAll("[^a-z ]", " ");
			line = line.trim().replaceAll(" +", " ");
			String[] split = line.split(" ");
			TreeMap<String, Integer> combine = new TreeMap<String, Integer>();
			for (int start = 0; start < split.length; start++) {
				for (int n = 1; n <= 5; n++) {
					if (n + start == split.length + 1) {
						break;
					}
					
					StringBuilder sb = new StringBuilder();
					for (int count = 0; count < n; count++) {
						sb.append(split[start + count]);
						if (count != n - 1) {
							sb.append(" ");
						}
					}
					
					String str = sb.toString();
					if (combine.containsKey(str)) {
						combine.put(str, combine.get(str) + 1);
					} else {
						combine.put(str, 1);
					}
				}
			}
			
			while (combine.size() != 0) {
				Entry<String, Integer> temp = combine.pollFirstEntry();
				word.set(temp.getKey());
				times.set(temp.getValue());
				context.write(word, times);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "NGram_Generation");
		job.setJarByClass(NGram.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}

