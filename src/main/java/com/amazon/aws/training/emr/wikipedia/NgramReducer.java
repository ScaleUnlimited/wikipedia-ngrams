/*
 * Copyright (c) 2011 Amazone
 * 
 * All rights reserved.
 */

package com.amazon.aws.training.emr.wikipedia;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class NgramReducer extends Reducer<Ngram, LongWritable, LongWritable, Ngram> {

    public static final int MIN_NGRAM_COUNT = 3;
    
    @Override
    protected void reduce(Ngram key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long counter = 0;
        for (LongWritable value : values) {
            counter += value.get();
        }

        // Only output counts > min count
        if (counter >= MIN_NGRAM_COUNT) {
            context.write(new LongWritable(counter), key);
            context.getCounter(NgramCounters.NGRAMS_UNIQUE_SAVED).increment(1);
        } else {
            context.getCounter(NgramCounters.NGRAMS_UNIQUE_SKIPPED).increment(1);
        }
    }
}
