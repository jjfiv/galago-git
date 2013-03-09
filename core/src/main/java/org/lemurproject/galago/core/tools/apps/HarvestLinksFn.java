/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.links.DataStreamWriter;
import org.lemurproject.galago.core.links.IndriHavestLinksWriter;
import org.lemurproject.galago.core.links.LinkDestNamer;
import org.lemurproject.galago.core.links.LinkExtractor;
import org.lemurproject.galago.core.links.UrlExtractor;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.DocumentUrl;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.MultiStep;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

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
            + "\tinputPath : [list of file paths | corpus]\n"
            + "\tacceptExternalUrls : [true|False]\n"
            + "\tacceptLocalLinks : [True|false]\n"
            + "\tacceptNoFollowLinks : [True|false]\n"
            + "\tindri : [true|False]\n"
            + "\tfilePrefix : /full/path/prefix/of/input/data/\t[only for 'indri']\n"
            + "\tprefixReplacement : /replacement/full/path/prefix/for/output/data/\t[only for 'indri']\n"
            + "\tgalago : [True|false]\n"
            + "\tgalagoDist : 5  [number of output shards]";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("inputPath")
            && !p.isList("inputPath", Type.STRING)) {
      output.println(getHelpString());
      return;
    }

    Job job = getHarvestLinksJob(p);

    if (job != null) {
      runTupleFlowJob(job, p, output);
    }

    output.println("Done harvesting link.");
  }

  private Job getHarvestLinksJob(Parameters p) throws IOException, Exception {
    Job job = new Job();

    // ensure we can extract links from tag: 'a'
    // sjh -- parameterize this to allow different link tags?
    p.set("tokenizer", new Parameters());
    p.getMap("tokenizer").set("fields", new ArrayList());
    p.getMap("tokenizer").getList("fields").add("a");
    p.getMap("tokenizer").getList("fields").add("base");

    // check indri
    p.set("indri", p.get("indri", false));
    p.set("galago", p.get("galago", true));
    assert (p.getBoolean("indri") || p.getBoolean("galago")) : "Must select indri or galago output, (or both)";

    // stage 1: split files

    Parameters splitParameters = p.get("parser", new Parameters());
    splitParameters.set("corpusPieces", p.get("distrib", 10));

    if (p.isString("filetype")) {
      splitParameters.set("filetype", p.getString("filetype"));
    }

    List<String> inputPaths = p.getAsList("inputPath");
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

      int dist = (int) p.get("galagoDist", 5);

      job.connect("parser", "namesWriter", ConnectionAssignmentType.Each, dist);
      job.connect("destNamer", "srcWriter", ConnectionAssignmentType.Each, dist);
      job.connect("destNamer", "destWriter", ConnectionAssignmentType.Each, dist);
    }


    return job;
  }

  private Stage getParserStage(Parameters p) throws IOException {
    Stage stage = new Stage("parser");

    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("docUrls", new DocumentUrl.UrlOrder());
    stage.addOutput("links", new ExtractedLink.DestUrlOrder());

    // parse and tokenize documents
    stage.add(new InputStep("splits")).add(BuildStageTemplates.getParserStep(p));
    stage.add(BuildStageTemplates.getTokenizerStep(p));

    MultiStep processingFork = new MultiStep();
    processingFork.addGroup("urls");
    processingFork.addGroup("lns");
    stage.add(processingFork);

    processingFork.addToGroup("urls", new Step(UrlExtractor.class));
    processingFork.addToGroup("urls", Utility.getSorter(new DocumentUrl.UrlOrder()));
    processingFork.addToGroup("urls", new OutputStep("docUrls"));

    processingFork.addToGroup("lns", new Step(LinkExtractor.class, p));
    processingFork.addToGroup("lns", Utility.getSorter(new ExtractedLink.DestUrlOrder()));
    processingFork.addToGroup("lns", new OutputStep("links"));

    if (p.getBoolean("galago")) {
      stage.addOutput("docNames", new DocumentUrl.IdentifierOrder());
      processingFork.addGroup("names");
      processingFork.addToGroup("names", new Step(UrlExtractor.class));
      processingFork.addToGroup("names", Utility.getSorter(new DocumentUrl.IdentifierOrder()));
      processingFork.addToGroup("names", new OutputStep("docNames"));
    }

    return stage;
  }

  private Stage getDestNamerStage(Parameters p) {
    Stage stage = new Stage("destNamer");

    stage.addInput("docUrls", new DocumentUrl.UrlOrder());
    stage.addInput("links", new ExtractedLink.DestUrlOrder());

    stage.add(new InputStep("links"));
    Parameters namerParams = new Parameters();
    namerParams.set("destNameStream", "docUrls");
    namerParams.set("acceptExternalUrls", p.get("acceptExternalUrls", false));
    stage.add(new Step(LinkDestNamer.class, namerParams));

    // need several copies of the output - each in a different order
    MultiStep processingFork = new MultiStep();
    stage.add(processingFork);

    if (p.getBoolean("indri")) {
      stage.addOutput("indriNamedLinks", new ExtractedLink.FilePathFileLocationOrder());
      processingFork.addGroup("indri");
      processingFork.addToGroup("indri", Utility.getSorter(new ExtractedLink.FilePathFileLocationOrder()));
      processingFork.addToGroup("indri", new OutputStep("indriNamedLinks"));
    }

    if (p.getBoolean("galago")) {
      stage.addOutput("srcLinks", new ExtractedLink.SrcNameOrder());
      processingFork.addGroup("srcLns");
      processingFork.addToGroup("srcLns", Utility.getSorter(new ExtractedLink.SrcNameOrder()));
      processingFork.addToGroup("srcLns", new OutputStep("srcLinks"));

      stage.addOutput("destLinks", new ExtractedLink.DestNameOrder());
      processingFork.addGroup("destLns");
      processingFork.addToGroup("destLns", Utility.getSorter(new ExtractedLink.DestNameOrder()));
      processingFork.addToGroup("destLns", new OutputStep("destLinks"));

      //.addGroup("srcName");
      // processingFork.addToGroup("srcName", Utility.getSorter(new ExtractedLink.FilePathFileLocationOrder()));
    }

    return stage;
  }

  private Stage getIndriCompatibleWriter(Parameters p) {
    Stage stage = new Stage("indriWriter");

    stage.addInput("indriNamedLinks", new ExtractedLink.FilePathFileLocationOrder());

    // I'm doing this manually to ensure the existence of these parameters early.
    Parameters writerParams = new Parameters();
    writerParams.set("filePrefix", p.getString("filePrefix"));
    writerParams.set("prefixReplacement", p.getString("prefixReplacement"));

    stage.add(new InputStep("indriNamedLinks"));
    stage.add(new Step(IndriHavestLinksWriter.class, writerParams));

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
    Parameters writerParams = new Parameters();
    writerParams.set("outputFolder", outputFolder);
    writerParams.set("outputFile", outputPrefix);
    writerParams.set("order", order.getName());
    writerParams.set("inputClass", type.getName());

    stage.add(new InputStep(streamName));
    stage.add(new Step(DataStreamWriter.class, writerParams));

    return stage;
  }
}
