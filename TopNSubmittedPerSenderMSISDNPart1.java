package com.adaptivemobile.epf;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.junit.Test;

public class TopNSubmittedPerSenderMSISDNPart1 {

	final String csvItem = "00,1,2,+353000000031,868000002,,1,1,-1,,,,,,2,,18";

	private static final Pattern csvPattern = Pattern
			.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?=,|$)");

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
		private LongWritable count = new LongWritable();
		private Text txtMsisdn = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			final String line = value.toString();

			final Matcher matcher = csvPattern.matcher(line);

			// duuhhhh !!
			matcher.find();
			matcher.find();
			matcher.find();
			matcher.find();

			final String msisdn = matcher.group();
			
			String countString = null;
			while (matcher.find()) {
				countString = matcher.group();
			}
			count.set(Integer.valueOf(countString));
			
			txtMsisdn.set(msisdn);
			output.collect(txtMsisdn, count);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new LongWritable(sum));
		}
	}
	
	
    @Test 
    public void test() {

		final Matcher matcher = csvPattern.matcher(csvItem);

		// duuhhhh !!
		matcher.find();
		matcher.find();
		matcher.find();
		matcher.find();

		final String msisdn = matcher.group();
		System.out.println(msisdn);
		
		String countString = null;
		while (matcher.find()) {
			countString = matcher.group();
			//System.out.println(countString);
		}
		
		System.out.println(Integer.valueOf(countString));
    }
    
}
