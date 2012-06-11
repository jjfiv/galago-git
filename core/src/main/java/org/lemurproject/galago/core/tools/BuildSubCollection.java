// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.index.SubCollectionWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.UniversalCounter;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 *
 * @author irmarc
 */
public class BuildSubCollection extends AppFunction {

  @Override
  public String getHelpString() {
      return "galago subcollection [flags] --filename=path <options> \n\n"
              + "Takes the same parameters as the build command, except\n"
              + "you must additionally specify the size of the collection\n"
              + "you want retained. This job builds a meta-input file that\n"
              + "can be used to build a subset of a full collection.";
  }

  private Job getIndexJob(Parameters buildParameters) throws Exception {
    Job job = new Job();

    // reading input files
    List<String> inputPaths = buildParameters.getAsList("inputPath");

    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", buildParameters.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));

    // split out, count 'em
    Stage stage = new Stage("countDocuments");
    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("counts", new KeyValuePair.KeyOrder());

    stage.add(new InputStep("splits"));
    stage.add(new Step(UniversalCounter.class, buildParameters));
    stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
    stage.add(new OutputStep("counts"));
    job.add(stage);

    // Final stage - count and make cutoff.
    stage = new Stage("cutoff");
    stage.addInput("counts", new KeyValuePair.KeyOrder());

    stage.add(new InputStep("counts"));
    stage.add(new Step(SubCollectionWriter.class, buildParameters));
    job.add(stage);

    // hook it up
    job.connect("inputSplit", "countDocuments", ConnectionAssignmentType.Each);
    job.connect("countDocuments", "cutoff", ConnectionAssignmentType.Combined);

    return job;
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("filename") && !p.isList("inputPath")) {
      output.println(getHelpString());
      return;
    }

    Job job;
    BuildSubCollection build = new BuildSubCollection();
    job = build.getIndexJob(p);

    App.runTupleFlowJob(job, p, output);
  }
}
