package com.adaptivemobile.epf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TopNSubmittedPerSenderMSISDNPart2 {
	final String csvItem = "+353000000025                   24";

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable count = new LongWritable();
		private Text txtMsisdn = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			final String line = value.toString();

			final String[] split = line.split("\\s+");
			
			count.set(Integer.valueOf(split[1]));
			txtMsisdn.set(split[0]);
			output.collect(count, txtMsisdn);
		}
	}



	public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		private static final int TOP_N = 10;
		private long recordsWritten = 0;

		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext() && (recordsWritten++ < TOP_N)) {
				output.collect(key, values.next());
			}
		}
	}
}
