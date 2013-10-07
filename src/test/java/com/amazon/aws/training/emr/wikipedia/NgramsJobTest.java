package com.amazon.aws.training.emr.wikipedia;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.scaleunlimited.wikipedia.SplitXmlTool;


public class NgramsJobTest extends Assert {

    @Test
    public void testProcessingWithHadoop() throws Exception {
        
        // First split some data.
        File outputDir = new File("build/test/NgramsJobTest/in");
        FileUtils.deleteDirectory(outputDir);
        outputDir.mkdirs();
        
        final int maxPagesPerSplit = 5;
        SplitXmlTool splittingTool = new SplitXmlTool(maxPagesPerSplit);
        splittingTool.run("src/test/resources/enwiki-snippet.xml", outputDir.getAbsolutePath(), false);

        // Now process that XML
        NgramsJob job = new NgramsJob();
        NgramsJobOptions processingOptions = new NgramsJobOptions();
        processingOptions.setInputFile(outputDir.getAbsolutePath());
        processingOptions.setOutputDir("build/test/NgramsJobTest/out");
        processingOptions.setNgramSize(2);
        
        // Only process a slice of the entries
        processingOptions.setPercent(20.0f);
        job.run(processingOptions, false);
        
        // Verify the results
        BufferedReader br = new BufferedReader(new FileReader("build/test/NgramsJobTest/out/" + NgramsJob.SORTED_SUBDIR_NAME + "/part-r-00000"));
        String curLine = br.readLine();
        assertNotNull(curLine);
        assertTrue(curLine.startsWith("e \t"));
        curLine = br.readLine();
        assertNotNull(curLine);
        assertTrue(curLine.startsWith(" a\t"));
        curLine = br.readLine();
        assertNotNull(curLine);
        assertTrue(curLine.startsWith("is\t"));
        
        br.close();
    }
    
}
