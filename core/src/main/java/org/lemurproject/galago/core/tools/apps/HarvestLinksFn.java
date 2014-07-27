/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.links.*;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.ExtractedLinkIndri;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author sjh
 */
public class HarvestLinksFn extends AppFunction {

  @Override
  public String getName() {
    return "harvest-links";
  }

  @Override
  public String getHelpString() {
    return "galago harvest-links <parameters>\n"
            + "\n"
            + "\tParameters:\n"
            + "\t--inputPath : [list of file paths | corpus]\n"
            + "\t--acceptExternalUrls : [true|False]\n"
            + "\t--acceptLocalLinks : [True|false]\n"
            + "\t--acceptNoFollowLinks : [True|false]\n"
            + "\t--indri : [true|False]\n"
            + "\t--filePrefix : /full/path/prefix/of/input/data/\t\t[only for 'indri']\n"
            + "\t--prefixReplacement : /replacement/full/path/prefix/for/indri/output/data/\t[only for 'indri']\n"
            + "\t--galago : [True|false]\n"
            + "\t--galagoDist : 5  [number of output shards]\n"
            + "\t--outputFolder : /path/to/galago/output/folder \t\t [only for 'galago']\n"
            + "\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("inputPath")
            && !p.isList("inputPath", String.class)) {
      output.println(getHelpString());
      return;
    }

    Job job = getHarvestLinksJob(p);

    if (job != null) {
      runTupleFlowJob(job, p, output);
    }

    output.println("Done harvesting link.");
  }

  private Job getHarvestLinksJob(Parameters p) throws Exception {
    Job job = new Job();

    // ensure we can extract links from tag: 'a'
    // sjh -- parameterize this to allow different link tags?
    p.set("tokenizer", Parameters.instance());
    p.getMap("tokenizer").set("fields", Arrays.asList("a", "base"));

    // check indri
    p.set("indri", p.get("indri", false));
    p.set("galago", p.get("galago", true));
    assert (p.getBoolean("indri") || p.getBoolean("galago")) : "Must select indri or galago output, (or both)";

    // stage 1: split files

    Parameters splitParameters = p.isMap("parser")? p.getMap("parser") : Parameters.instance();
    splitParameters.set("corpusPieces", p.get("distrib", 10));

    if (p.isString("filetype")) {
      splitParameters.set("filetype", p.getString("filetype"));
    }

    List<String> inputPaths = p.getAsList("inputPath", String.class);
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));

    // stage 2: parse files
    job.add(getParserStage(p));
    job.connect("inputSplit", "parser", ConnectionAssignmentType.Each);

    // stage 3: annotate dest-names
    job.add(getDestNamerStage(p));
    job.connect("parser", "destNamer", ConnectionAssignmentType.Each);


    if (p.getBoolean("indri")) {
      // stage 4.1: indri writer (filename/location order)
      job.add(getIndriCompatibleWriter(p));
      job.connect("destNamer", "indriWriter", ConnectionAssignmentType.Combined);
    }

    if (p.getBoolean("galago")) {
      // stage 4.2: url/names writer (+srcName, srcUrl)
      job.add(getGenericStreamWriter("namesWriter", "docNames", DocumentUrl.class, DocumentUrl.IdentifierOrder.class, p.getString("outputFolder") + File.separator + "names", "names"));
      // stage 4.3: links writer 1 (+srcName, srcUrl, destName, destUrl)
      job.add(getGenericStreamWriter("srcWriter", "srcLinks", ExtractedLink.class, ExtractedLink.SrcNameOrder.class, p.getString("outputFolder") + File.separator + "srcNameOrder", "links"));
      // stage 4.4: links writer 2 (+destURL, destName, srcUrl, srcName)
      job.add(getGenericStreamWriter("destWriter", "destLinks", ExtractedLink.class, ExtractedLink.DestNameOrder.class, p.getString("outputFolder") + File.separator + "destNameOrder", "links"));

      int dist = (int) p.get("galagoDist", 10);

      job.connect("parser", "namesWriter", ConnectionAssignmentType.Each, dist);
      job.connect("destNamer", "srcWriter", ConnectionAssignmentType.Each, dist);
      job.connect("destNamer", "destWriter", ConnectionAssignmentType.Each, dist);
    }


    return job;
  }

  private Stage getParserStage(Parameters p) throws IOException {
    Stage stage = new Stage("parser");

    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("docUrls", new DocumentUrl.UrlOrder(), CompressionType.GZIP);
    stage.addOutput("links", new ExtractedLinkIndri.DestUrlOrder(), CompressionType.GZIP);

    // parse and tokenize documents
    stage.add(new InputStepInformation("splits")).add(BuildStageTemplates.getParserStep(p));
    stage.add(BuildStageTemplates.getTokenizerStep(p));

    MultiStepInformation processingFork = new MultiStepInformation();
    processingFork.addGroup("urls");
    processingFork.addGroup("lns");
    stage.add(processingFork);

    processingFork.addToGroup("urls", new StepInformation(UrlExtractor.class));
    processingFork.addToGroup("urls", Utility.getSorter(new DocumentUrl.UrlOrder(), CompressionType.GZIP));
    processingFork.addToGroup("urls", new OutputStepInformation("docUrls"));

    processingFork.addToGroup("lns", new StepInformation(LinkExtractor.class, p));
    processingFork.addToGroup("lns", Utility.getSorter(new ExtractedLinkIndri.DestUrlOrder(), CompressionType.GZIP));
    processingFork.addToGroup("lns", new OutputStepInformation("links"));

    if (p.getBoolean("galago")) {
      stage.addOutput("docNames", new DocumentUrl.IdentifierOrder());
      processingFork.addGroup("names");
      processingFork.addToGroup("names", new StepInformation(UrlExtractor.class));
      processingFork.addToGroup("names", Utility.getSorter(new DocumentUrl.IdentifierOrder(), CompressionType.GZIP));
      processingFork.addToGroup("names", new OutputStepInformation("docNames"));
    }

    return stage;
  }

  private Stage getDestNamerStage(Parameters p) {
    Stage stage = new Stage("destNamer");

    stage.addInput("docUrls", new DocumentUrl.UrlOrder());
    stage.addInput("links", new ExtractedLinkIndri.DestUrlOrder());

    stage.add(new InputStepInformation("links"));
    Parameters namerParams = Parameters.instance();
    namerParams.set("destNameStream", "docUrls");
    namerParams.set("acceptExternalUrls", p.get("acceptExternalUrls", false));
    stage.add(new StepInformation(LinkDestNamer.class, namerParams));

    // need several copies of the output - each in a different order
    MultiStepInformation processingFork = new MultiStepInformation();
    stage.add(processingFork);

    if (p.getBoolean("indri")) {
      stage.addOutput("indriNamedLinks", new ExtractedLinkIndri.FilePathFileLocationOrder(), CompressionType.GZIP);
      processingFork.addGroup("indri");
      processingFork.addToGroup("indri", Utility.getSorter(new ExtractedLinkIndri.FilePathFileLocationOrder(), CompressionType.GZIP));
      processingFork.addToGroup("indri", new OutputStepInformation("indriNamedLinks"));
    }

    if (p.getBoolean("galago")) {
      stage.addOutput("srcLinks", new ExtractedLink.SrcNameOrder(), CompressionType.GZIP);
      processingFork.addGroup("srcLns");
      processingFork.addToGroup("srcLns", new StepInformation(ELItoEL.class));
      processingFork.addToGroup("srcLns", Utility.getSorter(new ExtractedLink.SrcNameOrder(), CompressionType.GZIP));
      processingFork.addToGroup("srcLns", new OutputStepInformation("srcLinks"));

      stage.addOutput("destLinks", new ExtractedLink.DestNameOrder(), CompressionType.GZIP);
      processingFork.addGroup("destLns");
      processingFork.addToGroup("destLns", new StepInformation(ELItoEL.class));
      processingFork.addToGroup("destLns", Utility.getSorter(new ExtractedLink.DestNameOrder(), CompressionType.GZIP));
      processingFork.addToGroup("destLns", new OutputStepInformation("destLinks"));

      //.addGroup("srcName");
      // processingFork.addToGroup("srcName", Utility.getSorter(new ExtractedLink.FilePathFileLocationOrder()));
    }

    return stage;
  }

  private Stage getIndriCompatibleWriter(Parameters p) {
    Stage stage = new Stage("indriWriter");

    stage.addInput("indriNamedLinks", new ExtractedLinkIndri.FilePathFileLocationOrder());

    // I'm doing this manually to ensure the existence of these parameters early.
    Parameters writerParams = Parameters.instance();
    writerParams.set("filePrefix", p.getString("filePrefix"));
    writerParams.set("prefixReplacement", p.getString("prefixReplacement"));

    stage.add(new InputStepInformation("indriNamedLinks"));
    stage.add(new StepInformation(IndriHavestLinksWriter.class, writerParams));

    return stage;
  }

  private Stage getGenericStreamWriter(
          String name, String streamName,
          Class<? extends org.lemurproject.galago.tupleflow.Type> type,
          Class<? extends Order> order,
          String outputFolder, String outputPrefix) throws Exception {

    Stage stage = new Stage(name);

    stage.addInput(streamName, order.getConstructor().newInstance());

    // I'm doing this manually to ensure the existence of these parameters early.
    Parameters writerParams = Parameters.instance();
    writerParams.set("outputFolder", outputFolder);
    writerParams.set("outputFile", outputPrefix);
    writerParams.set("order", order.getName());
    writerParams.set("inputClass", type.getName());
    writerParams.set("compression", "GZIP");

    stage.add(new InputStepInformation(streamName));
    stage.add(new StepInformation(DataStreamWriter.class, writerParams));

    return stage;
  }
}
