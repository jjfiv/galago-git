/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.contrib.document.DocumentFilter;
import org.lemurproject.galago.contrib.document.TrecWebDocumentWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.UniversalParser;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 *
 * @author sjh
 */
public class BuildFilteredCollection extends AppFunction {

  @Override
  public String getName() {
    return "filtered-collection";
  }

  @Override
  public String getHelpString() {
    return "galago filtered-collection [parameters]\n"
            + "\t--inputPath+/path/to/input/collection\n"
            + "\t--outputPath=/path/to/output/collection\n"
            + "\t--shardName=prefix for output shards\n"
            + "\t--outFileSize=[50 * 1024 * 1024]" // 50 mb uncompressed
            + "\t--filter+/path/to/filterfile\n"
            + "\t--compress=true\n"
            + "\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    // check parameters
    if (!p.containsKey("inputPath")
            || !p.isString("outputPath")
            || !p.containsKey("filter")) {
      output.println(getHelpString());
      return;
    }

    Job job = getIndexJob(p);

    runTupleFlowJob(job, p, output);
  }

  private static Job getIndexJob(Parameters buildParameters) throws Exception {

    // check filter parameters
    Parameters filterParameters = new Parameters();
    filterParameters.set("require", buildParameters.get("require", true));
    filterParameters.set("filter", new ArrayList());
    for (String path : (List<String>) buildParameters.getAsList("filter")) {
      filterParameters.getList("filter").add((new File(path).getAbsolutePath()));
    }

    Parameters outputParameters = new Parameters();
    outputParameters.set("outputPath", (new File(buildParameters.getString("outputPath")).getAbsolutePath()));
    outputParameters.set("shardName", buildParameters.get("shardName", "shard"));
    outputParameters.set("outFileSize", buildParameters.get("outFileSize", 50 * 1024 * 1024));
    outputParameters.set("compress", buildParameters.get("compress", true));

    File out = (new File(outputParameters.getString("outputPath")));
    if (out.isDirectory()) {
      Utility.deleteDirectory(out);
    } else if (out.isFile()) {
      out.delete();
    }
    out.mkdirs();

    Job job = new Job();

    // reading input files
    List<String> inputPaths = buildParameters.getAsList("inputPath");

    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", buildParameters.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileNameStartKeyOrder(), splitParameters));

    Stage stage = new Stage("writers");
    stage.addInput("splits", new DocumentSplit.FileNameStartKeyOrder());

    stage.add(new InputStep("splits"));
    stage.add(new Step(UniversalParser.class, buildParameters));
    stage.add(new Step(DocumentFilter.class, filterParameters));
    stage.add(new Step(TrecWebDocumentWriter.class, outputParameters));
    job.add(stage);

    // hook it up
    job.connect("inputSplit", "writers", ConnectionAssignmentType.Each);

    return job;
  }
}
