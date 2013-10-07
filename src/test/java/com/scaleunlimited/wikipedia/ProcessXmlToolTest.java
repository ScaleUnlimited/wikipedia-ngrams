package com.scaleunlimited.wikipedia;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import cascading.flow.Flow;


public class ProcessXmlToolTest {

    @Test
    public void testProcessing() throws Exception {
        
        // First split some data.
        File outputDir = new File("build/test/ProcessXmlToolTest/in");
        FileUtils.deleteDirectory(outputDir);
        outputDir.mkdirs();
        
        SplitXmlTool splittingTool = new SplitXmlTool(5);
        splittingTool.run("src/test/resources/enwiki-snippet.xml", outputDir.getAbsolutePath(), false);

        // Now process that XML
        ProcessXmlTool processingTool = new ProcessXmlTool();
        ProcessXmlOptions processingOptions = new ProcessXmlOptions();
        processingOptions.setInputFile(outputDir.getAbsolutePath());
        processingOptions.setOutputDir("build/test/ProcessXmlToolTest/out");
        Flow flow = processingTool.createFlow(processingOptions);
        flow.complete();
        
        // Verify the results
        // TODO KKr - make it so
    }
}
