package com.scaleunlimited.wikipedia;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class SplitXmlOptions {
    private boolean _debug = false;
    private boolean _jsonOuptut = false;

    private String _inputFile;
    private String _outputDir;
    
    @Option(name = "-debug", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debug) {
        _debug = debug;
    }

    public boolean isDebugLogging() {
        return _debug;
    }
    
    @Option(name = "-json", usage = "generate JSON", required = false)
    public void setJsonOutput(boolean jsonOutput) {
        _jsonOuptut = jsonOutput;
    }
    
    public boolean isJsonOutput() {
        return _jsonOuptut;
    }

    @Option(name = "-inputfile", usage = "path to Wikipedia dump file", required = true)
    public void setInputFile(String inputFile) {
        _inputFile = inputFile;
    }

    public String getInputFile() {
        return _inputFile;
    }
    
    @Option(name = "-outputdir", usage = "path to directory for results", required = true)
    public void setOutputDir(String outputDir) {
        _outputDir = outputDir;
    }

    public String getOutputDir() {
        return _outputDir;
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }


}
