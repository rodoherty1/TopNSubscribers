package com.adaptivemobile.epf;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;


public class TopNSubscribers {

    public static void main(String[] args) throws Exception {
        final JobControl jobControl = new JobControl("Rob's deadly, rapid JobControl Object");

        final String part1Output = args[1] + "_TopNPart1";        
        final Job job1 = createPart1(args[0], part1Output);

        final Job job2 = createPart2(part1Output, args[1]);
        job2.addDependingJob(job1);
        jobControl.addJob(job1);

        jobControl.addJob(job2);
        
        jobControl.run();
    }

	private static Job createPart1(String inputPath, String outputPath) throws IOException {
        JobConf conf1 = new JobConf(TopNSubscribers.class);
        conf1.setJobName("topNSubmittedPerSenderMSISDNPart1");

        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(LongWritable.class);

        conf1.setMapperClass(TopNSubmittedPerSenderMSISDNPart1.Map.class);
        conf1.setCombinerClass(TopNSubmittedPerSenderMSISDNPart1.Reduce.class);
        conf1.setReducerClass(TopNSubmittedPerSenderMSISDNPart1.Reduce.class);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf1, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf1, new Path(outputPath));
		return new Job(conf1);
	}

    
	private static Job createPart2(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(TopNSubmittedPerSenderMSISDNPart2.class);
        conf.setJobName("topNSubmittedPerSenderMSISDNPart2");

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(TopNSubmittedPerSenderMSISDNPart2.Map.class);
        conf.setCombinerClass(TopNSubmittedPerSenderMSISDNPart2.Reduce.class);
        conf.setReducerClass(TopNSubmittedPerSenderMSISDNPart2.Reduce.class);
        conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        return new Job(conf);
	}
}
