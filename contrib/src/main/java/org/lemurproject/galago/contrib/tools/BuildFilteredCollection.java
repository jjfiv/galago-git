/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.tools;

import org.lemurproject.galago.contrib.document.DocumentFilter;
import org.lemurproject.galago.contrib.document.TrecWebDocumentWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.UniversalParser;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.apps.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.execution.*;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

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
    Parameters filterParameters = Parameters.create();
    filterParameters.set("require", buildParameters.get("require", true));
    ArrayList<String> filters = new ArrayList<String>();
    for (String path : buildParameters.getAsList("filter", String.class)) {
      filters.add((new File(path).getAbsolutePath()));
    }
    filterParameters.set("filter", filters);

    Parameters outputParameters = Parameters.create();
    outputParameters.set("outputPath", (new File(buildParameters.getString("outputPath")).getAbsolutePath()));
    outputParameters.set("shardName", buildParameters.get("shardName", "shard"));
    outputParameters.set("outFileSize", buildParameters.get("outFileSize", 50 * 1024 * 1024));
    outputParameters.set("compress", buildParameters.get("compress", true));

    File out = (new File(outputParameters.getString("outputPath")));
    if (out.isDirectory()) {
      FSUtil.deleteDirectory(out);
    } else if (out.isFile()) {
      out.delete();
    }
    out.mkdirs();

    Job job = new Job();

    // reading input files
    List<String> inputPaths = buildParameters.getAsList("inputPath", String.class);

    Parameters splitParameters = Parameters.create();
    splitParameters.set("corpusPieces", buildParameters.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileNameStartKeyOrder(), splitParameters));

    Stage stage = new Stage("writers");
    stage.addInput("splits", new DocumentSplit.FileNameStartKeyOrder());

    stage.add(new InputStepInformation("splits"));
    stage.add(new StepInformation(UniversalParser.class, buildParameters));
    stage.add(new StepInformation(DocumentFilter.class, filterParameters));
    stage.add(new StepInformation(TrecWebDocumentWriter.class, outputParameters));
    job.add(stage);

    // hook it up
    job.connect("inputSplit", "writers", ConnectionAssignmentType.Each);

    return job;
  }
}
