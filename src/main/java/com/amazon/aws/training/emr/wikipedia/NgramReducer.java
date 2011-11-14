/*
 * Copyright (c) 2011 Amazone
 * 
 * All rights reserved.
 */

package com.amazon.aws.training.emr.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NgramReducer extends Reducer<ThreeGram, LongWritable, LongWritable, ThreeGram> {

    @Override
    protected void reduce(ThreeGram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long counter = 0;

        for (LongWritable value : values) {
            counter += value.get();
        }

        if (counter > 2) {
            context.write(new LongWritable(counter), key);
        }
    }
}
