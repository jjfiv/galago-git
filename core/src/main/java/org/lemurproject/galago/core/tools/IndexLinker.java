// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.lemurproject.galago.core.index.IndexLinkWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.IndexLinkGenerator;
import org.lemurproject.galago.core.tools.App.AppFunction;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.IndexLink;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

public class IndexLinker extends AppFunction {

    public Stage getGenerateLinksStage(String stageName,
				       String inStream,
				       String outStream,
				       Parameters parameters) {
	Stage stage = new Stage(stageName);

	stage.addInput(inStream, new DocumentSplit.FileIdOrder());
	stage.addOutput(outStream, new IndexLink.SrctypeTargettypeIdTargetidTargetposOrder());

	stage.add(new InputStep(inStream));
	stage.add(BuildStageTemplates.getParserStep(parameters));
	stage.add(new Step(IndexLinkGenerator.class));
	stage.add(Utility.getSorter(new IndexLink.SrctypeTargettypeIdTargetidTargetposOrder()));
	stage.add(new OutputStep(outStream));
	return stage;
    }

    public Stage getWriteLinksStage(String stageName,
				    String inStream,
				    Parameters parameters) {
	Stage stage = new Stage(stageName);
	stage.addInput(inStream, new IndexLink.SrctypeTargettypeIdTargetidTargetposOrder());

	stage.add(new InputStep(inStream));
	stage.add(new Step(IndexLinkWriter.class, parameters));
	return stage;
    }

  public Job getLinkJob(Parameters jobParameters) throws Exception {
      Job job = new Job();
      
      String linksPath = jobParameters.getString("linksPath");
      File manifest = new File(linksPath, "buildManifest.json");
      Utility.makeParentDirectories(manifest);
      Utility.copyStringToFile(jobParameters.toPrettyString(), manifest);

      List<String> inputPaths = jobParameters.getAsList("inputPath");
      Parameters splitParameters = jobParameters.get("parser", new Parameters()).clone();
      job.add(BuildStageTemplates.getSplitStage(inputPaths,
						DocumentSource.class,
						new DocumentSplit.FileIdOrder(),
						splitParameters));
      job.add(getGenerateLinksStage("generateLinks",
				    "splits",
				    "links",
				    jobParameters));
      Parameters writeParameters = new Parameters();
      writeParameters.set("filename", jobParameters.getString("linksPath"));
      job.add(getWriteLinksStage("writeLinks", "links", writeParameters));
      job.connect("inputSplit", "generateLinks", ConnectionAssignmentType.Each);
      job.connect("generateLinks", "writeLinks", ConnectionAssignmentType.Combined);
      return job;
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
      if (!p.isString("linksPath") && !p.isList("inputPath")) {
	  output.println(getHelpString());
	  return;
      }
           
    Job job;
    IndexLinker linker = new IndexLinker();
    job = linker.getLinkJob(p);

    if (job != null) {
	App.runTupleFlowJob(job, p, output);
    }
  }

  @Override
  public String getHelpString() {
    return "galago link-indexes [flags] --linksPath=<dir> (--inputPath+<input>)+\n\n"
            + "  Builds link structures for TEI files. Generalization later.\n\n"
            + "<input>:  Can be either a file or directory, and as many can be\n"
            + "          specified as you like.  Galago can read html, xml, txt, \n"
            + "          arc (Heritrix), warc, trectext, trecweb and corpus files.\n"
            + "          Files may be gzip compressed (.gz|.bz).\n"
	    + "<dir>:    The directory path for the produced links.\n\n"
            + App.getTupleFlowParameterString();
    //TODO: need to design parameters for field indexes + stemming for field indexes
  }
}