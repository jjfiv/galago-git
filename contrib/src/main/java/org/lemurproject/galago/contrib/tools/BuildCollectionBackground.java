/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.lemurproject.galago.contrib.index.disk.BackgroundStatsWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.WordCountReducer;
import org.lemurproject.galago.core.parse.WordCounter;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.apps.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.*;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

/**
 * builds a background language model from a set of documents - mapping from
 * term to count.
 *
 * @author sjh
 */
public class BuildCollectionBackground extends AppFunction {

  private Stage getParseStage(String name, String inputName, String outputName, Parameters p) throws Exception {
    Stage stage = new Stage(name);
    stage.addInput(inputName, new DocumentSplit.FileIdOrder());
    stage.addOutput(outputName, new WordCount.WordOrder());

    stage.add(new InputStepInformation(inputName));
    stage.add(BuildStageTemplates.getParserStep(p));
    stage.add(BuildStageTemplates.getTokenizerStep(p));

    if (p.containsKey("stemmer")) {
      Class stemmerClass = Class.forName(p.getString("stemmerClass"));
      stage.add(new StepInformation(stemmerClass));
    }

    stage.add(new StepInformation(WordCounter.class));
    stage.add(Utility.getSorter(new WordCount.WordOrder()));
    stage.add(new StepInformation(WordCountReducer.class));
    stage.add(new OutputStepInformation(outputName));

    return stage;
  }

  private Stage getWriterStage(String stageName, String inputName, Parameters writerParameters) {
    Stage stage = new Stage(stageName);

    stage.addInput(inputName, new WordCount.WordOrder());

    stage.add(new InputStepInformation(inputName));
    stage.add(new StepInformation(WordCountReducer.class));
    stage.add(new StepInformation(BackgroundStatsWriter.class, writerParameters));

    return stage;
  }

  public Job getBuildJob(Parameters p) throws Exception {
    // first check parameters
    File index = new File(p.getString("indexPath"));
    assert (index.isDirectory()) : getHelpString() + "\nindexPath must be an existing index.";
    p.set("indexPath", index.getAbsolutePath());

    List<String> inputs = p.getAsList("inputPath", String.class);

    String stemmer = p.get("stemmer", "krovetz");
    File output = new File(index, p.get("partName", "background." + stemmer));
    if (stemmer.equals("porter")) {
      p.set("stemmerClass", Porter2Stemmer.class.getName());
    } else if (stemmer.equals("krovetz")) {
      p.set("stemmerClass", KrovetzStemmer.class.getName());
    }

    Parameters writerParams = Parameters.create();
    writerParams.set("filename", output.getAbsolutePath());
    if (p.containsKey("stemmer")) {
      writerParams.set("stemmer", p.getString("stemmerClass"));
    }

    Job job = new Job();

    Parameters splitParameters = Parameters.create();
    splitParameters.set("corpusPieces", p.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputs, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));

    job.add(getParseStage("parser", "splits", "termCounts", p));
    job.add(getWriterStage("writer", "termCounts", writerParams));

    job.connect("inputSplit", "parser", ConnectionAssignmentType.Each);
    job.connect("parser", "writer", ConnectionAssignmentType.Combined);



    return job;
  }

  @Override
  public String getName() {
    return "build-coll-background";
  }

  @Override
  public String getHelpString() {
    return "galago build-background [flags] --indexPath=<index> (--inputPath=<input>)+\n\n"
            + "  Builds a Galago Structured Index Part file with TupleFlow,\n"
            + "<inputPath>:  Directory of input files. Files are assumed to be"
            + "              indexable using a standard 'build' process. >\n\n"
            + "<indexPath>:  The directory path of the index to add to.\n\n"
            + "Algorithm Flags:\n"
            + "  --partName={String}:      Sets the name of index part.\n"
            + "                            [default=background]\n"
            + "  --stemmer={porter|krovetz}: \n"
            + "                            Selects a stemmer class for the index part.\n"
            + "                            [default=krovetz]\n"
            + "\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // build-background index input
    if (!p.isString("indexPath") && !p.isList("indexPath")) {
      output.println(getHelpString());
    }

    Job job = getBuildJob(p);
    if (job != null) {
      runTupleFlowJob(job, p, output);
    }
  }

  public static void main(String[] args) throws Exception {
    new BuildCollectionBackground().run(Parameters.parseArgs(args), System.out);
  }
}
