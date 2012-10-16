/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentMappingData;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 *
 * @author sjh
 */
public class MergeIndex extends AppFunction {

  private static PrintStream output;
  private String outputPath;
  private List<String> inputPaths;

  // tupleflow stage functions
  private Stage getNumberIndexStage() {
    Stage stage = new Stage("indexNumberer");

    stage.addOutput("indexes", new DocumentSplit.FileIdOrder());

    Parameters p = new Parameters();
    p.set("inputPath", inputPaths);
    stage.add(new Step(IndexNumberer.class, p));
    stage.add(new OutputStep("indexes"));
    return stage;
  }

  private Stage getDocumentMappingStage(boolean renumberDocuments) {
    Stage stage = new Stage("documentMapper");

    stage.addInput("indexes", new DocumentSplit.FileIdOrder());
    stage.addOutput("documentMappingData", new DocumentMappingData.IndexIdOrder());

    stage.add(new InputStep("indexes"));
    if (renumberDocuments) {
      stage.add(new Step(DocumentNumberMapper.class));
    } else {
      stage.add(new Step(IdentityDocumentNumberMapper.class));
    }
    stage.add(new OutputStep("documentMappingData"));

    return stage;
  }

  private Stage getPartMerger(String stageName, String part, String outputFile) {
    Stage stage = new Stage(stageName);

    stage.addInput("indexes", new DocumentSplit.FileIdOrder());
    stage.addInput("documentMappingData", new DocumentMappingData.IndexIdOrder());

    stage.add(new InputStep("indexes"));
    Parameters p = new Parameters();
    p.set("mappingDataStream", "documentMappingData");
    p.set("part", part);
    p.set("filename", outputFile);
    stage.add(new Step(IndexPartMergeManager.class, p));

    return stage;
  }

  public Job getJob(Parameters p) throws IOException {
    Job job = new Job();

    this.outputPath = p.getString("indexPath");
    this.inputPaths = new ArrayList();
    for (String input : (List<String>) p.getAsList("inputPath")) {
      inputPaths.add((new File(input)).getAbsolutePath());
    }

    System.err.println(inputPaths.size() + " indexes found.");

    // get a list of shared mergable parts - by set intersection.
    HashSet<String> sharedParts = null;
    for (String index : (List<String>) p.getAsList("inputPath")) {
      DiskIndex i = new DiskIndex(index);
      Set<String> partNames = i.getPartNames();
      HashSet<String> mergableParts = new HashSet();
      for (String part : partNames) {
        if (i.getIndexPart(part).getManifest().containsKey("mergerClass")) {
          mergableParts.add(part);
        }
      }

      if (sharedParts == null) {
        sharedParts = new HashSet();
        sharedParts.addAll(mergableParts);
      }
      sharedParts.retainAll(mergableParts);
      i.close();
    }

    // log the parts to be merged.
    for (String part : sharedParts) {
      Logger.getLogger(getClass().getName()).log(Level.INFO, "Merging Part: " + part);
    }

    job.add(getNumberIndexStage());
    job.add(getDocumentMappingStage(p.get("renumberDocuments", true)));

    job.connect("indexNumberer", "documentMapper", ConnectionAssignmentType.Combined);

    for (String part : sharedParts) {
      job.add(getPartMerger(part + "MergeStage", part, outputPath + File.separator + part));
      job.connect("indexNumberer", part + "MergeStage", ConnectionAssignmentType.Combined);
      job.connect("documentMapper", part + "MergeStage", ConnectionAssignmentType.Combined);
    }

    return job;
  }

  // static main functions
  @Override
  public String getName(){
    return "merge-index";
  }
  
  @Override
  public String getHelpString() {
    return "galago merge-index \n\n"
            + "  Merges 2 or more indexes. Assumes that the document numberings\n"
            + "  are non-unique. So all documents are assigned new internal numbers.\n\n"
            + "Algorithm Flags:\n\n"
            + "  --inputPath+{/path/to/input} : Path to input index. Must supply two or more of this parameter.\n"
            + "  --indexPath={/path/to/output} : Path to output index.\n"
            + "  --renumberDocuments={true|false} : Boolean determines if new document identifiers should be generated.\n"
            + "                                   [default=true]\n\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if ((!p.containsKey("indexPath"))
        || !p.containsKey("inputPath")) {
      output.println(getHelpString());
      return;
    }

    MergeIndex build = new MergeIndex();
    Job job = build.getJob(p);
    runTupleFlowJob(job, p, output);
  }
}
