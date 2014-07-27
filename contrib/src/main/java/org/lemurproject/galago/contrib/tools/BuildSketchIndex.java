/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.lemurproject.galago.contrib.hash.UniversalStringHashFunction;
import org.lemurproject.galago.contrib.hash.WindowHasher;
import org.lemurproject.galago.contrib.index.InvertedSketchIndexWriter;
import org.lemurproject.galago.core.index.ExtractIndexDocumentNumbers;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.apps.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.core.window.ReduceNumberWordCount;
import org.lemurproject.galago.core.window.WindowProducer;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.*;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

/**
 *
 * @author sjh
 */
public class BuildSketchIndex extends AppFunction {

  @Override
  public String getName() {
    return "build-sketch-window";
  }

  @Override
  public String getHelpString() {
    return "galago build-sketch-window <<parameters>>\n"
            + "\t--inputPath+/path/to/input/files\n"
            + "\t--indexPath=/path/to/existing/index\n"
            + "\t--sketchIndexName=outputFileName\n"
            + "\t--depth=int [2]\n"
            + "\t--stemming=[boolean]\n"
            + "\t--stemmer=[org.lemurproject.galago.core.parse.stem.KrovetzStemmer]\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!(p.containsKey("inputPath")
            && p.containsKey("indexPath")
            && p.containsKey("sketchIndexName"))) {
      System.err.println(getHelpString());
    }

    Job job = getIndexJob(p);

    if (job != null) {
      runTupleFlowJob(job, p, output);
    }

    output.println("Done Indexing.");
  }

  private Job getIndexJob(Parameters buildParameters) throws Exception {

    Job job = new Job();

    String indexPath = new File(buildParameters.getString("indexPath")).getAbsolutePath();
    // ensure full paths
    buildParameters.set("indexPath", indexPath);

    List<String> inputPaths = buildParameters.getAsList("inputPath", String.class);
    Parameters splitParameters = buildParameters.get("parser", Parameters.instance());
    splitParameters.set("corpusPieces", buildParameters.get("distrib", 10));
    if (buildParameters.isMap("parser")) {
      splitParameters.set("parser", buildParameters.getMap("parser"));
    }

    if (buildParameters.isString("filetype")) {
      splitParameters.set("filetype", buildParameters.getString("filetype"));
    }

    // ensure stemmer is set
    if (buildParameters.get("stemming", false)) {
      buildParameters.set("stemmer", buildParameters.get("stemmer", KrovetzStemmer.class.getName()));
    }

    buildParameters.set("filename", indexPath + File.separator + buildParameters.getString("sketchIndexName"));

    // determine the sketch index parameters here, and create HashFunctions
    LocalRetrieval r = new LocalRetrieval(indexPath);
    FieldStatistics collectionStatistics = r.getCollectionStatistics("#lengths:document:part=lengths()");
    long collectionLength = collectionStatistics.collectionLength;
    long universe = 256; // each array unit is a byte //    

    Random rnd = new Random();
    int depth = (int) buildParameters.getLong("depth");
    double error = buildParameters.getDouble("error");

    Parameters hashFns = Parameters.instance();
    hashFns.set("depth", depth);
    hashFns.set("error", error);
    for (int i = 0; i < depth; i++) {
      UniversalStringHashFunction hash = UniversalStringHashFunction.generate(collectionLength, universe, error, rnd);
      hashFns.set(Integer.toString(i), hash.toParameters());
    }
    buildParameters.set("hashFns", hashFns);

    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));
    job.add(getParserStage(buildParameters));
    job.add(getWriterStage(buildParameters));

    job.connect("inputSplit", "parser", ConnectionAssignmentType.Each);
    job.connect("parser", "writer", ConnectionAssignmentType.Combined);

    return job;
  }

  private Stage getParserStage(Parameters buildParameters) throws ClassNotFoundException {
    Stage stage = new Stage("parser");

    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("postings", new NumberWordCount.WordDocumentOrder());

    stage.add(new InputStepInformation("splits"));
    stage.add(BuildStageTemplates.getParserStep(buildParameters));
    stage.add(BuildStageTemplates.getTokenizerStep(buildParameters));

    if (buildParameters.containsKey("stemmer")) {
      Class stemmer = Class.forName(buildParameters.getString("stemmer"));
      stage.add(BuildStageTemplates.getStemmerStep(Parameters.instance(), stemmer));
    }

    Parameters p = Parameters.instance();
    p.set("indexPath", buildParameters.getString("indexPath"));
    stage.add(new StepInformation(ExtractIndexDocumentNumbers.class, p));

    Parameters p2 = Parameters.instance();
    p2.set("n", buildParameters.getLong("n"));
    p2.set("width", buildParameters.getLong("width"));
    p2.set("ordered", buildParameters.getBoolean("ordered"));
    if (buildParameters.isString("fields") || buildParameters.isList("fields", String.class)) {
      p2.set("fields", buildParameters.getAsList("fields", String.class));
    }
    stage.add(new StepInformation(WindowProducer.class, p2));
    stage.add(new StepInformation(WindowHasher.class, buildParameters));
    stage.add(Utility.getSorter(new NumberWordCount.WordDocumentOrder()));
    stage.add(new StepInformation(ReduceNumberWordCount.class));

    stage.add(new OutputStepInformation("postings"));

    return stage;
  }

  private Stage getWriterStage(Parameters buildParameters) {
    Stage stage = new Stage("writer");

    stage.addInput("postings", new NumberWordCount.WordDocumentOrder());

    stage.add(new InputStepInformation("postings"));
    stage.add(new StepInformation(InvertedSketchIndexWriter.class, buildParameters));

    return stage;
  }
}
