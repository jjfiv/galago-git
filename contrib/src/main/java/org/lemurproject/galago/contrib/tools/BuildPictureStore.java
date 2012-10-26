// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.tools;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.contrib.index.PictureStoreWriter;
import org.lemurproject.galago.core.index.PictureDocumentWriter;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.PictureDocumentSerializer;
import org.lemurproject.galago.core.parse.PictureOccurrenceGenerator;
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
    stage.addInput(inStream, new DocumentSplit.FileIdOrder());
    stage.addOutput(outStream, new PictureOccurrence.IdOrdinalTopLeftOrder());
    stage.addOutput(outStream + "-docs", new KeyValuePair.KeyOrder());

    stage.add(new InputStep(inStream));
    stage.add(BuildStageTemplates.getParserStep(parameters));

    MultiStep fork = new MultiStep();

    ArrayList<Step> writeDocuments = new ArrayList<Step>();
    writeDocuments.add(new Step(PictureDocumentSerializer.class, parameters));
    writeDocuments.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
    writeDocuments.add(new OutputStep(outStream + "-docs"));
    fork.groups.add(writeDocuments);

    ArrayList<Step> outputOccurrences = new ArrayList<Step>();
    outputOccurrences.add(new Step(PictureOccurrenceGenerator.class));
    outputOccurrences.add(Utility.getSorter(new PictureOccurrence.IdOrdinalTopLeftOrder()));
    outputOccurrences.add(new OutputStep(outStream));
    fork.groups.add(outputOccurrences);

    stage.add(fork);
    return stage;
  }

  public Stage getWritePicturesStage(String stageName,
          String inStream,
          Parameters parameters) {
    Stage stage = new Stage(stageName);
    stage.addInput(inStream, new PictureOccurrence.IdOrdinalTopLeftOrder());

    stage.add(new InputStep(inStream));
    stage.add(new Step(PictureStoreWriter.class, parameters));
    return stage;
  }

  public Stage writePictureDocumentsStage(String stageName,
          String inStream,
          Parameters jobParameters) {
    Stage stage = new Stage(stageName);
    stage.addInput(inStream, new KeyValuePair.KeyOrder());
    stage.add(new InputStep(inStream));
    stage.add(new Step(PictureDocumentWriter.class, jobParameters));
    return stage;
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
            splitParameters));
    job.add(getGeneratePicturesStage("generatePictures",
            "splits",
            "pictures",
            jobParameters));
    Parameters writeParameters = new Parameters();
    writeParameters.set("filename", jobParameters.getString("picturesPath"));
    job.add(getWritePicturesStage("writePictures", "pictures", writeParameters));

    Parameters docParams = writeParameters.clone();
    String corpusPath = new File(jobParameters.getString("picturesPath"), "pictures.corpus").getCanonicalPath();
    docParams.set("filename", corpusPath);
    job.add(writePictureDocumentsStage("writeDocs", "pictures-docs", docParams));
    job.connect("inputSplit", "generatePictures", ConnectionAssignmentType.Each);
    job.connect("generatePictures", "writePictures", ConnectionAssignmentType.Combined);
    job.connect("generatePictures", "writeDocs", ConnectionAssignmentType.Combined);
    return job;
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