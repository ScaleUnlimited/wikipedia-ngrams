package com.scaleunlimited.wikipedia;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import net.arnx.jsonic.JSON;

import org.apache.commons.io.FileUtils;
import org.junit.Test;


public class SplitXmlToolTest {

    @Test
    public void testSnippetOfWikipedia() throws Exception {
        File outputDir = new File("build/test/SplitXmlToolTest/testSnippetOfWikipedia");
        FileUtils.deleteDirectory(outputDir);
        outputDir.mkdirs();
        
        SplitXmlTool tool = new SplitXmlTool(5);
        tool.run("src/test/resources/enwiki-snippet.xml", outputDir.getAbsolutePath(), false);
    }
    
    @Test
    public void testJsonGeneration() throws Exception {
        
        // First split some data into JSON
        File outputDir = new File("build/test/SplitXmlToolTest/testJsonGeneration");
        FileUtils.deleteDirectory(outputDir);
        outputDir.mkdirs();
        
        final int maxPagesPerSplit = 5;
        SplitXmlTool splittingTool = new SplitXmlTool(maxPagesPerSplit);
        splittingTool.run("src/test/resources/enwiki-snippet.xml", outputDir.getAbsolutePath(), true);

        // Now process that JSON
        File[] files = outputDir.listFiles();
        for (File file : files) {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            
            String curLine;
            while ((curLine = br.readLine()) != null) {
                JSON.decode(curLine);
            }
            
            br.close();
        }
    }

}
