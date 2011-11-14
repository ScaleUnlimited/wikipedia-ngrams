/*
 * Copyright (c) 011 Amazon, Inc.
 * 
 * All rights reserved.
 */
package com.amazon.aws.training.emr.wikipedia;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NgramsMapper extends Mapper<LongWritable, Text, ThreeGram, LongWritable> {

    @Override
    protected void map(LongWritable lineNum, Text line, Context context) throws IOException, InterruptedException {

        char[] chars = line.toString().toCharArray();
        
        LongWritable value = new LongWritable(1);
        ThreeGram key = new ThreeGram(' ', ' ', ' ');
        
        for (char c : chars) {
            key.shift(c);
            context.write(key, value);
        }
    }
}
