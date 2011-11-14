package com.scaleunlimited.wikipedia;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.bixolabs.cascading.NullContext;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.MultiMapReducePlanner;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class ProcessXmlTool {

    private static final Fields FIELDS = new Fields("title", "author");
    
    @SuppressWarnings("serial")
    private static class ParseXml extends BaseOperation<NullContext> implements Function<NullContext> {
        private static final Logger LOGGER = Logger.getLogger(ParseXml.class);
        
        private transient SAXReader _reader = null;

        public ParseXml() {
            super(FIELDS);
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _reader = new SAXReader();
            _reader.setEncoding("UTF-8");
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            String inputLine = functionCall.getArguments().getString("line");
            
            Document doc;
            try {
                doc = _reader.read(new StringReader(inputLine));
                Node titleNode = doc.selectSingleNode("/page/title");
                String title = titleNode == null ? "unknown" : titleNode.getText();
                
                Node authorNode = doc.selectSingleNode("/page/revision/contributor/username");
                String author = authorNode == null ? "unknown" : authorNode.getText();
                functionCall.getOutputCollector().add(new Tuple(title, author));
            } catch (DocumentException e) {
                LOGGER.error("Can't parse input line: " + inputLine, e);
            }
            
        }
        
    }
    public Flow createFlow(ProcessXmlOptions options) throws IOException, InterruptedException {
        // Avoid having Hadoop wind up trying to use the Jaxen parser, which will
        // trigger exceptions that look like "Failed to set setXIncludeAware(true) for parser blah"
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
                           "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

        JobConf conf = new JobConf();
        conf.set("mapred.child.java.opts", "-server");
        
        Properties props = new Properties();
        FlowConnector.setApplicationJarClass(props, ProcessXmlTool.class);

        if (!options.isDebugLogging()) {
            props.put("log4j.logger", "cascading=INFO");
        }
        
        MultiMapReducePlanner.setJobConf(props, conf);

        Tap sourceTap = new Hfs(new TextLine(), options.getInputFile());
        
        Tap sinkTap = new Hfs(new SequenceFile(FIELDS), options.getOutputDir(), SinkMode.REPLACE);
        
        Pipe xmlPipe = new Pipe("xml");
        xmlPipe = new Each(xmlPipe, new ParseXml());
        xmlPipe = new Each(xmlPipe, new Debug());
        
        FlowConnector flowConnector = new FlowConnector(props);
        return flowConnector.connect(sourceTap, sinkTap, xmlPipe);
    }
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    public static void main(String[] args) {
        ProcessXmlOptions options = new ProcessXmlOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
            ProcessXmlTool tool = new ProcessXmlTool();
            Flow flow = tool.createFlow(options);
            flow.complete();
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        } catch (Throwable t) {
            System.err.println(t.getMessage());
            System.exit(-1);
        }
   }

}
