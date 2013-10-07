/*
 * Copyright (c) 011 Amazon, Inc.
 * 
 * All rights reserved.
 */
package com.amazon.aws.training.emr.wikipedia;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

public class NgramsMapper extends Mapper<LongWritable, Text, Ngram, LongWritable> {
    private static final Logger LOGGER = Logger.getLogger(NgramsMapper.class);
    
    public static final String NGRAM_SIZE_KEY = "ngrams.size";
    public static final String PAGE_PERCENT = "page.percent";
    
    private transient SAXReader _reader = null;
    private transient int _ngramSize;
    private transient float _percent;
    private transient Random _rand;
    
    @Override
    protected void setup(Mapper<LongWritable, Text, Ngram, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        _reader = new SAXReader();
        _reader.setEncoding("UTF-8");
        
        _ngramSize = context.getConfiguration().getInt(NGRAM_SIZE_KEY, NgramsJobOptions.DEFAULT_NGRAM_SIZE);
        _percent = context.getConfiguration().getFloat(PAGE_PERCENT, NgramsJobOptions.DEFAULT_PERCENT);
        
        // We'll fix the random seed so that our tests are reproducible
        _rand = new Random(1L);
    }
    
    @Override
    protected void map(LongWritable lineNum, Text line, Context context) throws IOException, InterruptedException {

        if (_rand.nextFloat() > _percent) {
            // Skip the page
            context.getCounter(NgramCounters.PAGES_SKIPPED).increment(1);
            return;
        }
        
        Document doc;
        try {
            doc = _reader.read(new ByteArrayInputStream(line.getBytes(), 0, line.getLength()));
            context.getCounter(NgramCounters.PAGES_PARSED).increment(1);
            Node textNode = doc.selectSingleNode("/page/revision/text");
            if (textNode != null) {
                char[] chars = textNode.getText().toCharArray();
                LongWritable value = new LongWritable(1);
                Ngram key = new Ngram(_ngramSize);
                
                long totalNgrams = 0;
                for (char c : chars) {
                    key.shift(c);
                    context.write(key, value);
                    totalNgrams += 1;
                }
                
                context.getCounter(NgramCounters.NGRAMS_CREATED).increment(totalNgrams);
            } else {
                LOGGER.debug("No /page/revision/text element for " + line.toString());
            }
        } catch (DocumentException e) {
            context.getCounter(NgramCounters.PAGES_FAILED).increment(1);
            LOGGER.error("Can't parse input line: " + line.toString(), e);
        }
    }
}
