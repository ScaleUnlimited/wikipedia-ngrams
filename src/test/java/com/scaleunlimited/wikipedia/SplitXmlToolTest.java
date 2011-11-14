package com.scaleunlimited.wikipedia;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;


public class SplitXmlToolTest {

    @Test
    public void testSnippetOfWikipedia() throws Exception {
        File outputDir = new File("build/test/SplitXmlToolTest");
        FileUtils.deleteDirectory(outputDir);
        outputDir.mkdirs();
        
        SplitXmlTool tool = new SplitXmlTool(5);
        tool.run("src/test/resources/enwiki-snippet.xml", outputDir.getAbsolutePath());
    }
}
