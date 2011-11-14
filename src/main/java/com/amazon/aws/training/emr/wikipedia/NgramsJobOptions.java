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
    public static final int DEFAULT_REDUCERS = 0;
    
    private String inputFile;
    private String outputDir;
    
    private int numReducers = DEFAULT_REDUCERS;
    
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

    public String getInputFile() {
        return inputFile;
    }

    public String getOuputDir() {
        return outputDir;
    }

    public int getNumReducers() {
        return numReducers;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
