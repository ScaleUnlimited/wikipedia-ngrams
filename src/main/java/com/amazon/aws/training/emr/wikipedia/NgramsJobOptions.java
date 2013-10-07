/*
 * Copyright (c) 2009-2011 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.amazon.aws.training.emr.wikipedia;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class NgramsJobOptions {
    // With 1 reducer, we'll get one result file with sorted ngrams
    // Using more than 1 reducer means you'll get multiple output files,
    // each with a sampling of the resulting ngrams
    public static final int DEFAULT_REDUCERS = 1;
    
    // The default size of 2 means we're generating bigrams, or
    // character pairs. Bigger values here (e.g. 3 for trigrams)
    // will result in longer processing time.
    public static final int DEFAULT_NGRAM_SIZE = 2;
    
    // What percentage of the Wikipedia pages we want to process.
    // By default we want 100% (1.0). Smaller values are useful
    // for reducing the total processing time.
    public static final float DEFAULT_PERCENT = 1.0f;
    
    private String inputFile;
    private String outputDir;
    
    private int numReducers = DEFAULT_REDUCERS;
    private int ngramSize = DEFAULT_NGRAM_SIZE;
    private float percent = DEFAULT_PERCENT;
    
    @Option(name = "-inputfile", usage = "path to file to process", required = true)
    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    @Option(name = "-outputdir", usage = "path to directory for results", required = true)
    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    @Option(name = "-numreducers", usage = "number of reducers", required = false)
    public void setNumReducers(int numReducers) {
        this.numReducers = numReducers;
    }

    @Option(name = "-ngramsize", usage = "size of ngram", required = false)
    public void setNgramSize(int ngramSize) {
        this.ngramSize = ngramSize;
    }

    @Option(name = "-percent", usage = "percentage of pages to process (e.g. 50.0 for half of all pages)", required = false)
    public void setPercent(float percent) {
        this.percent = percent / 100.0f;
    }

    
    public String getInputFile() {
        return inputFile;
    }

    public String getOuputDir() {
        return outputDir;
    }

    public int getNumReducers() {
        return numReducers;
    }

    public int getNgramSize() {
        return ngramSize;
    }
    
    public float getPercent() {
        return percent;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
