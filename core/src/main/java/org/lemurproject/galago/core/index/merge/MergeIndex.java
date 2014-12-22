/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.types.DocumentMappingData;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.execution.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
public class MergeIndex extends AppFunction {

  private String outputPath;
  private List<String> inputPaths;

  // tupleflow stage functions
  private Stage getNumberIndexStage() {
    Stage stage = new Stage("indexNumberer");

    stage.addOutput("indexes", new DocumentSplit.FileIdOrder());

    Parameters p = Parameters.create();
    p.set("inputPath", inputPaths);
    stage.add(new StepInformation(IndexNumberer.class, p));
    stage.add(new OutputStepInformation("indexes"));
    return stage;
  }

  private Stage getDocumentMappingStage(boolean renumberDocuments) {
    Stage stage = new Stage("documentMapper");

    stage.addInput("indexes", new DocumentSplit.FileIdOrder());
    stage.addOutput("documentMappingData", new DocumentMappingData.IndexIdOrder());

    stage.add(new InputStepInformation("indexes"));
    if (renumberDocuments) {
      stage.add(new StepInformation(DocumentNumberMapper.class));
    } else {
      stage.add(new StepInformation(IdentityDocumentNumberMapper.class));
    }
    stage.add(new OutputStepInformation("documentMappingData"));

    return stage;
  }

  private Stage getPartMerger(String stageName, String part, String outputFile) {
    Stage stage = new Stage(stageName);

    stage.addInput("indexes", new DocumentSplit.FileIdOrder());
    stage.addInput("documentMappingData", new DocumentMappingData.IndexIdOrder());

    stage.add(new InputStepInformation("indexes"));
    Parameters p = Parameters.create();
    p.set("mappingDataStream", "documentMappingData");
    p.set("part", part);
    p.set("filename", outputFile);
    stage.add(new StepInformation(IndexPartMergeManager.class, p));

    return stage;
  }

  public Job getJob(Parameters p) throws IOException {
    Job job = new Job();

    this.outputPath = p.getString("indexPath");
    this.inputPaths = new ArrayList<>();
    for (String input : p.getAsList("inputPath", String.class)) {
      inputPaths.add((new File(input)).getAbsolutePath());
    }

    System.err.println(inputPaths.size() + " indexes found.");

    // get a list of shared mergable parts - by set intersection.
    HashSet<String> sharedParts = null;
    for (String index : p.getAsList("inputPath", String.class)) {
      DiskIndex i = new DiskIndex(index);
      Set<String> partNames = i.getPartNames();
      HashSet<String> mergableParts = new HashSet<>();
      for (String part : partNames) {
        if (i.getIndexPart(part).getManifest().containsKey("mergerClass")) {
          mergableParts.add(part);
        }
      }

      if (sharedParts == null) {
        sharedParts = new HashSet<>();
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
