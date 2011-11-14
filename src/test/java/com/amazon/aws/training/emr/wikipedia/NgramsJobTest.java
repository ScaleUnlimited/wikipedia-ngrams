package com.amazon.aws.training.emr.wikipedia;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.scaleunlimited.wikipedia.SplitXmlTool;


public class NgramsJobTest {

    @Test
    public void testProcessingWithHadoop() throws Exception {
        
        // First split some data.
        File outputDir = new File("build/test/NgramsJobTest/in");
        FileUtils.deleteDirectory(outputDir);
        outputDir.mkdirs();
        
        SplitXmlTool splittingTool = new SplitXmlTool(5);
        splittingTool.run("src/test/resources/enwiki-snippet.xml", outputDir.getAbsolutePath());

        // Now process that XML
        NgramsJob job = new NgramsJob();
        NgramsJobOptions processingOptions = new NgramsJobOptions();
        processingOptions.setInputFile(outputDir.getAbsolutePath());
        processingOptions.setOutputDir("build/test/NgramsJobTest/out");
        job.run(processingOptions, false);
        
        // Verify the results
        // TODO KKr - make it so
    }
}
