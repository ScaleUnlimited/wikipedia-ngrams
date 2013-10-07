/*
 * Copyright (c) 2011 Amazone
 * 
 * All rights reserved.
 */

package com.amazon.aws.training.emr.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducer extends Reducer<LongWritable, Ngram, Ngram, LongWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Ngram> values, Context context) throws IOException, InterruptedException {

        // All we want to do is reverse the output
        for (Ngram value : values) {
            context.write(value, key);
        }
    }
}
