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

public class SortingMapper extends Mapper<ThreeGram, LongWritable, LongWritable, ThreeGram> {

    @Override
    protected void map(ThreeGram ngram, LongWritable count, Context context) throws IOException, InterruptedException {

        context.write(count, ngram);
    }
}
