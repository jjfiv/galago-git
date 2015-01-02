/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.index.disk.ConflationIndexWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.stem.ConflationExtractor;
import org.lemurproject.galago.core.parse.stem.ConflationReducer;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.TupleflowAppUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.*;

/**
 *
 * @author sjh
 */
public class BuildStemmerConflation extends AppFunction {

  @Override
  public String getName() {
    return "stemmer-conflation";
  }

  @Override
  public String getHelpString() {
    return "galago stemmer-conflation [flags] --outputPath=<outputPath> --stemmer=<stemmer> "
            + "                                (--inputPath+<input>)+\n\n"
            + "  Builds an index part that maps a stemmed term to a list of "
            + "  conflated terms. \n\n"
            + "<input>:  Can be either a file or directory, and as many can be\n"
            + "          specified as you like.  Galago can read html, xml, txt, \n"
            + "          arc (Heritrix), warc, trectext, trecweb and corpus files.\n"
            + "          Files may be gzip compressed (.gz|.bz).\n"
            + "<outputPath>:  The path of the index part to produce.\n"
            + "<stemmer>: Name of a stemmer; [porter, krovetz, ...]\n\n"
            + TupleflowAppUtil.getTupleFlowParameterString();
    //TODO: need to design parameters for field indexes + stemming for field indexes
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("output") && !p.isList("inputPath") && !p.isString("stemmer")) {
      output.println(getHelpString());
      return;
    }

    Job job;
    job = getIndexJob(p);

    if (job != null) {
      TupleflowAppUtil.runTupleFlowJob(job, p, output);
    }

  }

  private Job getIndexJob(Parameters p) throws IOException {
    Job job = new Job();

    List<String> inputPaths = p.getAsList("inputPath");
    File output = new File(p.getString("outputPath"));

    Parameters splitParameters = Parameters.create();
    splitParameters.set("corpusPieces", p.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));
    job.add(getParserStage(p));
    job.add(getWriterStage(output));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
    job.connect("parsePostings", "writerStage", ConnectionAssignmentType.Combined);

    return job;
  }

  private Stage getParserStage(Parameters p) throws IOException {
    Stage stage = new Stage("parsePostings");

    // connections
    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("conflations", new KeyValuePair.KeyValueOrder());

    // Steps
    stage.add(new InputStepInformation("splits"));
    stage.add(BuildStageTemplates.getParserStep(p));
    stage.add(BuildStageTemplates.getTokenizerStep(p));

    Parameters conflationParams = Parameters.create();
    conflationParams.set("stemmerClass", getStemmerClass(p.getString("stemmer")));

    stage.add(new StepInformation(ConflationExtractor.class, conflationParams));
    stage.add(Utility.getSorter(new KeyValuePair.KeyValueOrder()));
    stage.add(new StepInformation(ConflationReducer.class));
    stage.add(new OutputStepInformation("conflations"));

    return stage;
  }

  private Stage getWriterStage(File output) {
    Stage stage = new Stage("writerStage");

    stage.addInput("conflations", new KeyValuePair.KeyValueOrder());
    stage.add(new InputStepInformation("conflations"));

    // repeat the discard step - over the newly combined data
    stage.add(new StepInformation(ConflationReducer.class));

    Parameters writerParams = Parameters.create();
    writerParams.set("filename", output.getAbsolutePath());
    stage.add(new StepInformation(ConflationIndexWriter.class, writerParams));

    return stage;
  }

  private String getStemmerClass(String stemmer) {
    if (stemmer.startsWith("porter")) {
      return Porter2Stemmer.class.getName();
    }
    if (stemmer.startsWith("krovetz")) {
      return KrovetzStemmer.class.getName();
    }
    throw new RuntimeException("BuildStemmerConflation.class - Failed to find a class for stemmer " + stemmer);
  }
}
