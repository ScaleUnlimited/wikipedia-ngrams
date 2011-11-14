package com.scaleunlimited.wikipedia;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.InvalidParameterException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public class SplitXmlTool {

    private static final int DEFAULT_MAX_PAGES_PER_PART = 100000;

    private int _maxPagesPerPart;
    
    public SplitXmlTool() {
        this(DEFAULT_MAX_PAGES_PER_PART);
    }
    
    public SplitXmlTool(int maxPagesPerPart) {
        _maxPagesPerPart = maxPagesPerPart;
    }
    
    public void run(String inputFile, String outputDirPath) throws IOException {
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
        bw = makePartFileWriter(outputDir, numParts);

        while ((curLine = br.readLine()) != null) {
            if (!inPage) {
                if (curLine.equals("  <page>")) {
                    inPage = true;
                    curPage.append("<page>");
                }
            } else if (curLine.equals("  </page>")) {
                curPage.append("</page>\r");
                bw.write(curPage.toString());
                curPage.delete(0, curPage.length());
                inPage = false;

                numLines += 1;
                if (numLines  == _maxPagesPerPart) {
                    bw.close();
                    numLines = 0;

                    numParts += 1;
                    System.out.println("Writing to part #" + numParts);
                    bw = makePartFileWriter(outputDir, numParts);
                }
            } else {
                curPage.append(curLine);
            }
        }

        // Close the current part file.
        bw.close();
    }
    
    private static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }

    private BufferedWriter makePartFileWriter(File outputDir, int partNumber) throws FileNotFoundException {
        File f = new File(outputDir, String.format("part-%03d.xml", partNumber));
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
    }

    public static void main(String[] args) {
        SplitXmlOptions options = new SplitXmlOptions();
        CmdLineParser parser = new CmdLineParser(options);
        
        try {
            parser.parseArgument(args);
            SplitXmlTool tool = new SplitXmlTool();
            tool.run(options.getInputFile(), options.getOutputDir());
        } catch(CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        } catch (Throwable t) {
            System.err.println(t.getMessage());
            System.exit(-1);
        }
   }
}
