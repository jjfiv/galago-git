/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.disk.BackgroundLMWriter;
import org.lemurproject.galago.core.parse.DocumentNumberer;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.WordCountReducer;
import org.lemurproject.galago.core.parse.WordCounter;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.MultiStep;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 * builds a background language model from a set of documents - mapping from
 * term to count.
 *
 * @author sjh
 */
public class BuildBackground extends AppFunction {

  private static Stage getParseStage(Parameters p) throws Exception {
    Stage stage = new Stage("inputParser");
    stage.addInput("splits", new DocumentSplit.FileIdOrder());

    stage.add(new InputStep("splits"));
    stage.add(BuildStageTemplates.getParserStep(p));
    stage.add(BuildStageTemplates.getTokenizerStep(p));

    // actual document numbers will not be written to index
    // they are only used to count the total number of documents.
    stage.add(new Step(DocumentNumberer.class));

    MultiStep fork = new MultiStep();

    if (p.get("nonStemmedPostings", true)) {
      stage.addOutput("nonStemmedTermCounts", new WordCount.WordOrder());

      ArrayList<Step> nonstemmed = new ArrayList();
      nonstemmed.add(new Step(WordCounter.class));
      nonstemmed.add(Utility.getSorter(new WordCount.WordOrder()));
      nonstemmed.add(new Step(WordCountReducer.class));
      nonstemmed.add(new OutputStep("nonStemmedTermCounts"));
      fork.groups.add(nonstemmed);
    }

    if (p.getBoolean("stemmedPostings")) {
      for (String stemmer : (List<String>) p.getList("stemmer")) {
        stage.addOutput("stemmedTermCounts-" + stemmer, new WordCount.WordOrder());

        ArrayList<Step> stemmed = new ArrayList();

        Class stemmerClass = Class.forName(p.getMap("stemmerClass").getString(stemmer));

        stemmed.add(new Step(stemmerClass));
        stemmed.add(new Step(WordCounter.class));
        stemmed.add(Utility.getSorter(new WordCount.WordOrder()));
        stemmed.add(new Step(WordCountReducer.class));
        stemmed.add(new OutputStep("stemmedTermCounts-" + stemmer));
        fork.groups.add(stemmed);
      }
    }


    stage.add(fork);
    return stage;
  }

  private static Stage getWriterStage(String stageName, String inputName, Parameters writerParameters) {
    Stage stage = new Stage(stageName);

    stage.addInput(inputName, new WordCount.WordOrder());

    stage.add(new InputStep(inputName));
    stage.add(new Step(WordCountReducer.class));
    stage.add(new Step(BackgroundLMWriter.class, writerParameters));

    return stage;
  }

  public static Job getBuildJob(Parameters p) throws Exception {
    // using the same as BuildIndex - ensures same schema.
    // some additional parameters will be set ( they will be ignored )
    p = BuildIndex.checkBuildIndexParameters(p);
    if (p == null) {
      throw new RuntimeException("Failed to parse parameters correctly.");
    }

    // first check parameters
    String indexPath = p.getString("indexPath");
    assert (new File(indexPath).isDirectory());
    List<String> inputs = p.getAsList("inputPath");
    String output = indexPath + File.separator + p.get("partName", "background");

    Parameters writerParams = new Parameters();
    writerParams.set("filename", output);

    Job job = new Job();

    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", p.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputs, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));

    job.add(getParseStage(p));

    job.connect("inputSplit", "inputParser", ConnectionAssignmentType.Each);

    if (p.getBoolean("nonStemmedPostings")) {
      Parameters params = writerParams.clone();
      params.set("filename", output);
      job.add(getWriterStage("nonStemmedWriter", "nonStemmedTermCounts", params));

      job.connect("inputParser", "nonStemmedWriter", ConnectionAssignmentType.Combined);
    }

    if (p.getBoolean("stemmedPostings")) {
      for (String stemmer : (List<String>) p.getList("stemmer")) {
        Parameters params = writerParams.clone();
        params.set("filename", output + "." + stemmer);
        params.set("stemmer", p.getMap("stemmerClass").getString(stemmer));

        job.add(getWriterStage("stemmedWriter-" + stemmer, "stemmedTermCounts-" + stemmer, params));

        job.connect("inputParser", "stemmedWriter-" + stemmer, ConnectionAssignmentType.Combined);
      }
    }

    return job;
  }

  public String getName() {
    return "build-background";
  }

  public String getHelpString() {
    return "galago build-background [flags] --indexPath=<index> (--inputPath=<input>)+\n\n"
            + "  Builds a Galago Structured Index Part file with TupleFlow,\n"
            + "<inputPath>:  One or more indicator files in format:\n"
            + "           < document-identifier \t [log-probability] >\n\n"
            + "<indexPath>:  The directory path of the index to add to.\n\n"
            + "Algorithm Flags:\n"
            + "  --partName={String}:      Sets the name of index part.\n"
            + "                            [default=background]\n"
            + "  --stemmer+{none|porter|korvetz}: \n"
            + "                            Selects a set of stemmer classes for the index part.\n"
            + "                            [default=porter]\n"
            + "\n"
            + getTupleFlowParameterString();
  }

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
    new BuildBackground().run(new Parameters(args), System.out);
  }
}
