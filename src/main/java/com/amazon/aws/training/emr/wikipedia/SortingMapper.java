/*
 * Copyright (c) 011 Amazon, Inc.
 * 
 * All rights reserved.
 */
package com.amazon.aws.training.emr.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SortingMapper extends Mapper<Ngram, LongWritable, LongWritable, Ngram> {

    @Override
    protected void map(Ngram ngram, LongWritable count, Context context) throws IOException, InterruptedException {

        context.write(count, ngram);
    }
}
