package com.scaleunlimited.wikipedia;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class SplitXmlOptions {
    private boolean _debug;
    private String _inputFile;
    private String _outputDir;
    
    @Option(name = "-debug", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debug) {
        _debug = debug;
    }

    @Option(name = "-inputfile", usage = "path to Wikipedia dump file", required = true)
    public void setInputFile(String inputFile) {
        _inputFile = inputFile;
    }

    @Option(name = "-outputdir", usage = "path to directory for results", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    public boolean isDebugLogging() {
        return _debug;
    }
    
    public String getInputFile() {
        return _inputFile;
    }
    
    public String getOutputDir() {
        return _outputDir;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }


}
