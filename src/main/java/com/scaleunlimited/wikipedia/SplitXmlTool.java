package com.scaleunlimited.wikipedia;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.security.InvalidParameterException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class SplitXmlTool {
    private static final Logger LOGGER = Logger.getLogger(SplitXmlTool.class);
    
    private static final int DEFAULT_MAX_PAGES_PER_PART = 100000;

    private int _maxPagesPerPart;
    
    public SplitXmlTool() {
        this(DEFAULT_MAX_PAGES_PER_PART);
    }
    
    public SplitXmlTool(int maxPagesPerPart) {
        _maxPagesPerPart = maxPagesPerPart;
    }
    
    public void run(String inputFile, String outputDirPath, boolean jsonOutput) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));

        BufferedWriter bw = null;
        String curLine = null;
        boolean inPage = false;
        int numLines = 0;
        int numParts = 0;
        StringBuilder curPage = new StringBuilder();

        File outputDir = new File(outputDirPath);
        if (!outputDir.exists()) {
            throw new InvalidParameterException("Output directory must exist: " + outputDir);
        } else if (!outputDir.isDirectory()) {
            throw new InvalidParameterException("Output directory can't be a file: " + outputDir);
        }

        System.out.println("Writing to part #" + numParts);
        bw = makePartFileWriter(outputDir, numParts, jsonOutput);

        int lineNum = 0;
        while ((curLine = br.readLine()) != null) {
            lineNum += 1;
            
            if (!inPage) {
                if (curLine.equals("  <page>")) {
                    inPage = true;
                    curPage.append("<page>");
                }
            } else if (curLine.equals("  </page>")) {
                curPage.append("</page>\n");


                try {
                    String output = curPage.toString();
                    if (jsonOutput) {
                        output = convertXmlToJson(output);
                    }

                    bw.write(output);
                } catch (DocumentException e) {
                    LOGGER.error(String.format("Couldn't convert line %d", lineNum), e);
                }

                curPage.delete(0, curPage.length());
                inPage = false;

                numLines += 1;
                if (numLines  == _maxPagesPerPart) {
                    bw.close();
                    numLines = 0;

                    numParts += 1;
                    System.out.println("Writing to part #" + numParts);
                    bw = makePartFileWriter(outputDir, numParts, jsonOutput);
                }
            } else {
                curPage.append(curLine);
            }
        }

        // Close the current part file.
        bw.close();
    }
    
    private void appendNode(Document doc, String xpath, StringWriter sw, JSONWriter writer) throws IOException {
        appendNode(doc, xpath, sw, writer, false, false);
    }
    
    private void appendNode(Document doc, String xpath, StringWriter sw, JSONWriter writer, boolean parentAsPrefix, boolean lastNode) throws IOException {
        Node node = doc.selectSingleNode(xpath);
        if (parentAsPrefix) {
            node.setName(node.getParent().getName() + "-" + node.getName());
        }
        
        writer.write(node);
        writer.flush();
        
        if (!lastNode) {
            sw.append(", ");
        }
    }
    
    private String convertXmlToJson(String xml) throws DocumentException, IOException {
        SAXReader reader = new SAXReader();
        reader.setEncoding("UTF-8");
        
        InputStream is = IOUtils.toInputStream(xml, "UTF-8");
        Document doc = reader.read(is);
        StringWriter sw = new StringWriter();
        sw.append("{ ");
        
        JSONWriter writer = new JSONWriter(sw);

        appendNode(doc, "/page/title", sw, writer);
        appendNode(doc, "/page/id", sw, writer);
        appendNode(doc, "/page/revision/id", sw, writer, true, false);
        appendNode(doc, "/page/revision/timestamp", sw, writer, true, false);
        appendNode(doc, "/page/revision/contributor/username", sw, writer, true, false);
        appendNode(doc, "/page/revision/text", sw, writer, true, true);
        
        sw.append(" }\n");
        return sw.toString();
    }
    
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    private BufferedWriter makePartFileWriter(File outputDir, int partNumber, boolean jsonOutput) throws FileNotFoundException {
        String suffix = jsonOutput ? "json" : "xml";
        File f = new File(outputDir, String.format("part-%03d.%s", partNumber, suffix));
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
    }

    public static void main(String[] args) {
        SplitXmlOptions options = new SplitXmlOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
            SplitXmlTool tool = new SplitXmlTool();
            tool.run(options.getInputFile(), options.getOutputDir(), options.isJsonOutput());
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        } catch (Throwable t) {
            System.err.println(t.getMessage());
            System.exit(-1);
        }
   }
}
