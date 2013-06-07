// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.IOException;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.WordDateCount;
import org.lemurproject.galago.core.index.WordDateCountWriter;
import org.lemurproject.galago.core.parse.*;

public class BuildWordDateIndex extends AppFunction {

  public Stage getGenerateWordDatesStage(String name,
          String inStream,
          String outStream,
          Parameters p) {
    Stage stage = new Stage(name);
    stage.addInput(inStream, new DocumentSplit.FileIdOrder());
    stage.addOutput(outStream, new WordDateCount.WordDateOrder());

    stage.add(new InputStep(inStream));
    stage.add(BuildStageTemplates.getParserStep(p));
    stage.add(new Step(DateTokenizer.class));
    stage.add(Utility.getSorter(new WordDateCount.WordDateOrder(),
            WordDateReducer.class));
    stage.add(new OutputStep(outStream));
    return stage;
  }

  public Stage getWriteWordDatesStage(String name,
          String inStream,
          Parameters p) throws IOException {
    Stage stage = new Stage(name);
    stage.addInput(inStream, new WordDateCount.WordDateOrder());
    stage.add(new InputStep(inStream));
    Parameters copy = p.clone();
    copy.set("filename", new File(p.getString("filename"), "postings").getCanonicalPath());
    stage.add(new Step(WordDateCountWriter.class, copy));
    return stage;
  }

  public Job getWordDateJob(Parameters jobParameters) throws Exception {
    Job job = new Job();

    String wdPath = jobParameters.getString("indexPath");
    File manifest = new File(wdPath, "buildManifest.json");
    Utility.makeParentDirectories(manifest);
    Utility.copyStringToFile(jobParameters.toPrettyString(), manifest);

    List<String> inputPaths = jobParameters.getAsList("inputPath");
    Parameters splitParameters = jobParameters.isMap("parser")? jobParameters.getMap("parser") : new Parameters();
    job.add(BuildStageTemplates.getSplitStage(inputPaths,
            DocumentSource.class,
            new DocumentSplit.FileIdOrder(),
            splitParameters));
    job.add(getGenerateWordDatesStage("generateWordDates",
            "splits",
            "worddates",
            jobParameters));
    Parameters writeParameters = new Parameters();
    writeParameters.set("filename", jobParameters.getString("indexPath"));
    job.add(getWriteWordDatesStage("writeWordDates", "worddates", writeParameters));

    job.connect("inputSplit", "generateWordDates", ConnectionAssignmentType.Each);
    job.connect("generateWordDates", "writeWordDates", ConnectionAssignmentType.Combined);
    return job;
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("path") && !p.isList("inputPath")) {
      output.println(getHelpString());
      return;
    }

    Job job;
    BuildWordDateIndex dIndex = new BuildWordDateIndex();
    job = dIndex.getWordDateJob(p);

    if (job != null) {
      runTupleFlowJob(job, p, output);
    }
  }

  @Override
  public String getName() {
    return "build-word-dates";
  }

  @Override
  public String getHelpString() {
    return "galago build-word-dates [flags] --indexPath=<dir> (--inputPath+<input>)+\n\n"
            + " Creates an index that stores the occurrences of words over time.\n\n"
            + "<input>:  Can be either a file or directory, and as many can be\n"
            + "          specified as you like.  Galago can read html, xml, txt, \n"
            + "          arc (Heritrix), warc, trectext, trecweb and corpus files.\n"
            + "          Files may be gzip compressed (.gz|.bz).\n"
            + "<dir>:    The directory path for the produced pictures.\n\n"
            + getTupleFlowParameterString();
  }
}