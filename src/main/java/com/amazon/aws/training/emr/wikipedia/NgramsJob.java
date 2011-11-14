/*
 * Copyright (c) 2009-2011 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.amazon.aws.training.emr.wikipedia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;

public class NgramsJob {

    private static final String RAW_SUBDIR_NAME = "raw-counts";
    private static final String SORTED_SUBDIR_NAME = "sorted-counts";
    
    private void generateNgrams(NgramsJobOptions options, boolean printConfig) throws IOException, ClassNotFoundException, InterruptedException {
        // create Hadoop path instances
        Path inputPath = new Path(options.getInputFile());
        Path outputPath = new Path(options.getOuputDir());
        Path tempDirPath = new Path(outputPath, RAW_SUBDIR_NAME);
        
        // Create the job configuration
        Configuration conf = new Configuration();

        // get the FileSystem instances for each path
        // this allows for the paths to live on different FileSystems (local, hdfs, s3, etc)
        FileSystem inputFS = inputPath.getFileSystem(conf);
        FileSystem outputFS = tempDirPath.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(inputPath)) {
            System.out.println("Input file does not exist: " + inputPath);
            System.exit(-1);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(tempDirPath)) {
            outputFS.delete(tempDirPath, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "wikipedia ngrams job");
        // finds the enclosing jar path
        job.setJarByClass(NgramsJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextOutputFormat.setOutputPath(job, tempDirPath);

        // our mapper class
        job.setMapperClass(NgramsMapper.class);
        job.setMapOutputKeyClass(ThreeGram.class);
        job.setMapOutputValueClass(LongWritable.class);

        // our reducer class
        job.setReducerClass(NgramReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ThreeGram.class);
        
        // The default number of reducer tasks (slots) is set in mapred-default.xml,
        // and optionally customized in mapred-site.xml. It's possible to override
        // this on a per-job basis.
        if (options.getNumReducers() != NgramsJobOptions.DEFAULT_REDUCERS) {
            job.setNumReduceTasks(options.getNumReducers());
        }

        if (printConfig) {
            System.out.println("starting wikipedia-ngrams job using:");
            System.out.println(" jobtracker    = " + job.getConfiguration().get("mapred.job.tracker"));
            System.out.println(" inputPath     = " + inputPath.makeQualified(inputFS));
            System.out.println(" outputPath    = " + tempDirPath.makeQualified(outputFS));
            System.out.println(" num reducers  = " + job.getNumReduceTasks());
            System.out.println(" comparator    = " + job.getSortComparator().getClass().getName());
            System.out.println(" mapper class  = " + job.getMapperClass());
            System.out.println(" reducer class = " + job.getReducerClass());
            System.out.println("");
        }

        // run job and block until job is done
        job.waitForCompletion(false);
    }
    
    private void sortNgrams(NgramsJobOptions options, boolean printConfig) throws IOException, ClassNotFoundException, InterruptedException {
        // create Hadoop path instances
        Path parentPath = new Path(options.getOuputDir());
        Path inputPath = new Path(parentPath, RAW_SUBDIR_NAME);
        Path outputPath = new Path(parentPath, SORTED_SUBDIR_NAME);
        
        // Create the job configuration
        Configuration conf = new Configuration();

        // get the FileSystem instances for each path
        // this allows for the paths to live on different FileSystems (local, hdfs, s3, etc)
        FileSystem inputFS = inputPath.getFileSystem(conf);
        FileSystem outputFS = outputPath.getFileSystem(conf);

        // if input path does not exists, fail
        if (!inputFS.exists(inputPath)) {
            System.out.println("Input file does not exist: " + inputPath);
            System.exit(-1);
        }

        // if output path exists, delete recursively
        if (outputFS.exists(outputPath)) {
            outputFS.delete(outputPath, true);
        }

        // Create the actual job and run it.
        Job job = new Job(conf, "sort ngrams job");
        // finds the enclosing jar path
        job.setJarByClass(NgramsJob.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.setInputPaths(job, inputPath);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        // We don't specify a mapper, which means it's the identity mapper.
        // But we do have to specify the classes for key/value
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ThreeGram.class);
        
        // our reducer class
        job.setReducerClass(SortingReducer.class);

        // Use a custom comparator to sort high to low.
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        
        // The default number of reducer tasks (slots) is set in mapred-default.xml,
        // and optionally customized in mapred-site.xml. It's possible to override
        // this on a per-job basis.
        if (options.getNumReducers() != NgramsJobOptions.DEFAULT_REDUCERS) {
            job.setNumReduceTasks(options.getNumReducers());
        }

        if (printConfig) {
            System.out.println("starting wikipedia-ngrams job using:");
            System.out.println(" jobtracker    = " + job.getConfiguration().get("mapred.job.tracker"));
            System.out.println(" inputPath     = " + inputPath.makeQualified(inputFS));
            System.out.println(" outputPath    = " + outputPath.makeQualified(outputFS));
            System.out.println(" num reducers  = " + job.getNumReduceTasks());
            System.out.println(" comparator    = " + job.getSortComparator().getClass().getName());
            System.out.println(" mapper class  = " + job.getMapperClass());
            System.out.println(" reducer class = " + job.getReducerClass());
            System.out.println("");
        }

        // run job and block until job is done
        job.waitForCompletion(false);
    }
    
    public void run(NgramsJobOptions options, boolean printConfig) throws IOException {
        
        try {
            generateNgrams(options, printConfig);
        } catch (Exception e) {
            System.err.println("Exception running job to count ngrams: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }

        try {
            sortNgrams(options, printConfig);
        } catch (Exception e) {
            System.err.println("Exception running job to sort ngrams: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }
    }
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    public static void main(String[] args) throws IOException {
        NgramsJobOptions options = new NgramsJobOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }

        NgramsJob job = new NgramsJob();
        job.run(options, true);
    }

}
