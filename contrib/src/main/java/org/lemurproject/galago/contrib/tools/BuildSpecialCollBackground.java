/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.contrib.index.disk.BackgroundStatsWriter;
import org.lemurproject.galago.contrib.parse.ParseWordCountString;
import org.lemurproject.galago.contrib.parse.WordCountCleaner;
import org.lemurproject.galago.contrib.parse.WordCountStemmer;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.WordCountReducer;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.apps.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 * builds a background language model from a set of documents - mapping from
 * term to count.
 *
 * @author sjh
 */
public class BuildSpecialCollBackground extends AppFunction {

  private Stage getParseStage(String name, String inputName, String outputName, Parameters p) throws Exception {
    Stage stage = new Stage(name);
    stage.addInput(inputName, new DocumentSplit.FileIdOrder());
    stage.addOutput(outputName, new WordCount.WordOrder());

    stage.add(new InputStep(inputName));

    Parameters parseParams = new Parameters();
    parseParams.set("delim", p.get("delim", "\t"));
    parseParams.set("lower", p.get("lower", true));
    parseParams.set("stripPunct", p.get("stripPunct", true));
    parseParams.set("replacements", new Parameters());
    parseParams.getMap("replacements").set("_","~");
    parseParams.getMap("replacements").set(" ","~");
    
    stage.add(new Step(ParseWordCountString.class, parseParams));
    stage.add(new Step(WordCountCleaner.class, parseParams));
    
    if (p.containsKey("stemmer")) {
      Parameters stemParams = new Parameters();
      stemParams.set("stemmer", p.getString("stemmerClass"));
      stage.add(new Step(WordCountStemmer.class, stemParams));
    }

    stage.add(Utility.getSorter(new WordCount.WordOrder()));
    stage.add(new Step(WordCountReducer.class));
    stage.add(new OutputStep(outputName));

    return stage;
  }

  private Stage getWriterStage(String stageName, String inputName, Parameters writerParameters) {
    Stage stage = new Stage(stageName);

    stage.addInput(inputName, new WordCount.WordOrder());

    stage.add(new InputStep(inputName));
    stage.add(new Step(WordCountReducer.class));
    stage.add(new Step(BackgroundStatsWriter.class, writerParameters));

    return stage;
  }

  public Job getBuildJob(Parameters p) throws Exception {
    // first check parameters
    File index = new File(p.getString("indexPath"));
    assert (index.isDirectory()) : getHelpString() + "\nindexPath must be an existing index.";
    p.set("indexPath", index.getAbsolutePath());

    List<String> inputs = p.getAsList("inputPath");

    String stemmer = p.get("stemmer", "krovetz");
    File output = new File(index, p.get("partName", "background." + stemmer));
    if (stemmer.equals("porter")) {
      p.set("stemmerClass", Porter2Stemmer.class.getName());
    } else if (stemmer.equals("krovetz")) {
      p.set("stemmerClass", KrovetzStemmer.class.getName());
    }

    Parameters writerParams = new Parameters();
    writerParams.set("filename", output.getAbsolutePath());
    if (p.containsKey("stemmer")) {
      writerParams.set("stemmer", p.getString("stemmerClass"));
    }

    Job job = new Job();

    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", p.get("distrib", 10));
    splitParameters.set("filetype", "misc");
    job.add(BuildStageTemplates.getSplitStage(inputs, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));

    job.add(getParseStage("parser", "splits", "termCounts", p));
    job.add(getWriterStage("writer", "termCounts", writerParams));

    job.connect("inputSplit", "parser", ConnectionAssignmentType.Each);
    job.connect("parser", "writer", ConnectionAssignmentType.Combined);

    return job;
  }

  @Override
  public String getName() {
    return "build-special-coll-background";
  }

  @Override
  public String getHelpString() {
    return "galago build-background [flags] --indexPath=<index> (--inputPath=<input>)+\n\n"
            + "  Builds a Galago Structured Index Part file with TupleFlow,\n"
            + "<inputPath>:  One or more files in the format:\n"
            + "           < document-identifier\tcf\tdc\tmax_df >\n"
            + "             Missing fields will be assumed to be '1'.\n"
            + "             Deliminator is assumed to be '\\t'.\n"
            + "<indexPath>:  The directory path of the index to add to.\n\n"
            + "Algorithm Flags:\n"
            + "  --partName={String}:      Sets the name of index part.\n"
            + "                            [default=background]\n"
            + "  --delim={String}:         Sets the deliminator for the input files.\n"
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
    new BuildSpecialCollBackground().run(Parameters.parseArgs(args), System.out);
  }
}
