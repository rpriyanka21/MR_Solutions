package com.java.violation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class LawSectionMapper extends MapReduceLaw implements Mapper <LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] IndividualLawSection = valueString.split(",");
		output.collect(new Text(IndividualLawSection[27]+"-"+IndividualLawSection[28]), one);
	}
}
