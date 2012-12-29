// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.contrib.index.PictureStoreWriter;
import org.lemurproject.galago.contrib.index.PictureDocumentWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import ciir.proteus.galago.parse.PictureDocumentSerializer;
import ciir.proteus.galago.parse.PictureOccurrenceGenerator;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.PictureOccurrence;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.MultiStep;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

public class BuildPictureStore extends AppFunction {

  public Stage getGeneratePicturesStage(String stageName,
          String inStream,
          String outStream,
          Parameters parameters) {
    Stage stage = new Stage(stageName);
    stage.addInput(inStream, new DocumentSplit.FileIdOrder())
	.addOutput(outStream, new PictureOccurrence.IdOrdinalTopLeftOrder())
	.addOutput(outStream + "-docs", new KeyValuePair.KeyOrder())
	.add(new InputStep(inStream))
	.add(BuildStageTemplates.getParserStep(parameters));

    MultiStep fork = new MultiStep();
    String name = "picture docs";
    fork.addGroup(name)
	.addToGroup(name, new Step(PictureDocumentSerializer.class, parameters))
	.addToGroup(name, Utility.getSorter(new KeyValuePair.KeyOrder()))
	.addToGroup(name, new OutputStep(outStream + "-docs"));
    
    name = "picture occurrences";
    fork.addGroup(name)
	.addToGroup(name, new Step(PictureOccurrenceGenerator.class))
	.addToGroup(name, Utility.getSorter(new PictureOccurrence.IdOrdinalTopLeftOrder()))
	.addToGroup(name, new OutputStep(outStream));

    return stage.add(fork);
  }

  public Stage getWritePicturesStage(String stageName,
          String inStream,
          Parameters parameters) {
    return new Stage(stageName)
	.addInput(inStream, new PictureOccurrence.IdOrdinalTopLeftOrder())
	.add(new InputStep(inStream))
	.add(new Step(PictureStoreWriter.class, parameters));
  }

  public Stage writePictureDocumentsStage(String stageName,
          String inStream,
          Parameters jobParameters) {
      return new Stage(stageName)
	  .addInput(inStream, new KeyValuePair.KeyOrder())
	  .add(new InputStep(inStream))
	  .add(new Step(PictureDocumentWriter.class, jobParameters));
  }

  public Job getPicturesJob(Parameters jobParameters) throws Exception {
    Job job = new Job();

    String picturesPath = jobParameters.getString("picturesPath");
    File manifest = new File(picturesPath, "buildManifest.json");
    Utility.makeParentDirectories(manifest);
    Utility.copyStringToFile(jobParameters.toPrettyString(), manifest);

    List<String> inputPaths = jobParameters.getAsList("inputPath");
    Parameters splitParameters = jobParameters.get("parser", new Parameters()).clone();
    job.add(BuildStageTemplates.getSplitStage(inputPaths,
					      DocumentSource.class,
					      new DocumentSplit.FileIdOrder(),
					      splitParameters))
	.add(getGeneratePicturesStage("generatePictures",
				      "splits",
				      "pictures",
				      jobParameters));
    Parameters writeParameters = new Parameters();
    writeParameters.set("filename", jobParameters.getString("picturesPath"));
    job.add(getWritePicturesStage("writePictures", "pictures", writeParameters));

    Parameters docParams = writeParameters.clone();
    String corpusPath = new File(jobParameters.getString("picturesPath"), "pictures.corpus").getCanonicalPath();
    docParams.set("filename", corpusPath);
    return job.add(writePictureDocumentsStage("writeDocs", "pictures-docs", docParams))
	.connect("inputSplit", "generatePictures", ConnectionAssignmentType.Each)
	.connect("generatePictures", "writePictures", ConnectionAssignmentType.Combined)
	.connect("generatePictures", "writeDocs", ConnectionAssignmentType.Combined);
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.isString("picturesPath") && !p.isList("inputPath")) {
      output.println(getHelpString());
      return;
    }

    Job job;
    BuildPictureStore pStore = new BuildPictureStore();
    job = pStore.getPicturesJob(p);

    if (job != null) {
      runTupleFlowJob(job, p, output);
    }
  }

  @Override
  public String getName() {
    return "build-pictures";
  }

  @Override
  public String getHelpString() {
    return "galago build-pictures [flags] --picturesPath=<dir> (--inputPath+<input>)+\n\n"
            + "  Builds picture structures for TEI files. Generalization later.\n\n"
            + "<input>:  Can be either a file or directory, and as many can be\n"
            + "          specified as you like.  Galago can read html, xml, txt, \n"
            + "          arc (Heritrix), warc, trectext, trecweb and corpus files.\n"
            + "          Files may be gzip compressed (.gz|.bz).\n"
            + "<dir>:    The directory path for the produced pictures.\n\n"
            + getTupleFlowParameterString();
  }
}