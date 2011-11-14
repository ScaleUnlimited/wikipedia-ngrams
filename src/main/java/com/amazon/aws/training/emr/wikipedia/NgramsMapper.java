/*
 * Copyright (c) 011 Amazon, Inc.
 * 
 * All rights reserved.
 */
package com.amazon.aws.training.emr.wikipedia;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

public class NgramsMapper extends Mapper<LongWritable, Text, ThreeGram, LongWritable> {
    private static final Logger LOGGER = Logger.getLogger(NgramsMapper.class);
    
    private transient SAXReader _reader = null;

    @Override
    protected void setup(Mapper<LongWritable, Text, ThreeGram, LongWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        
        _reader = new SAXReader();
        _reader.setEncoding("UTF-8");
    }
    
    @Override
    protected void map(LongWritable lineNum, Text line, Context context) throws IOException, InterruptedException {

        Document doc;
        try {
            doc = _reader.read(new ByteArrayInputStream(line.getBytes(), 0, line.getLength()));
            Node textNode = doc.selectSingleNode("/page/revision/text");
            if (textNode != null) {
                char[] chars = textNode.getText().toCharArray();
                LongWritable value = new LongWritable(1);
                ThreeGram key = new ThreeGram(' ', ' ', ' ');
                
                for (char c : chars) {
                    key.shift(c);
                    context.write(key, value);
                }
            } else {
                LOGGER.debug("No /page/revision/text element for " + line.toString());
            }
        } catch (DocumentException e) {
            LOGGER.error("Can't parse input line: " + line.toString(), e);
        }
    }
}
