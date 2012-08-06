// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.index.corpus.SplitBTreeKeyWriter;
import org.lemurproject.galago.core.index.corpus.CorpusFolderWriter;
import org.lemurproject.galago.core.index.corpus.DocumentToKeyValuePair;
import org.lemurproject.galago.core.index.corpus.CorpusFileWriter;
import org.lemurproject.galago.core.index.corpus.KeyValuePairToDocument;
import org.lemurproject.galago.core.parse.DocumentNumberer;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.ConnectionPointType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.StageConnectionPoint;
import org.lemurproject.galago.tupleflow.execution.Step;

/*
 * @author sjh
 * 
 * new corpus structure;
 *  - contained within some folder.
 *  - corpus data is stored in some set of files (.cds)
 *  - index file stores: document-name --> (file, offset)
 * 
 */
public class MakeCorpus {

  public static Job getCorpusFileJob(String outputCorpus, List<String> inputs, Parameters corpusParameters) throws IOException {
    Job job = new Job();

    Stage stage = new Stage("make-corpus");

    Parameters p = new Parameters();
    ArrayList<String> inputFiles = new ArrayList<String>();
    ArrayList<String> inputDirectories = new ArrayList<String>();
    for (String input : inputs) {
      File inputFile = new File(input);

      if (inputFile.isFile()) {
        inputFiles.add(inputFile.getAbsolutePath());
      } else if (inputFile.isDirectory()) {
        inputDirectories.add(inputFile.getAbsolutePath());
      } else {
        throw new IOException("Couldn't find file/directory: " + input);
      }
      p.set("filename", inputFiles);
      p.set("directory", inputDirectories);
    }

    stage.add(new Step(DocumentSource.class, p));
    stage.add(BuildStageTemplates.getParserStep(corpusParameters));
    stage.add(BuildStageTemplates.getTokenizerStep(corpusParameters));
    stage.add(new Step(DocumentNumberer.class));

    stage.add(new Step(DocumentToKeyValuePair.class));
    stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
    stage.add(new Step(KeyValuePairToDocument.class));
    p = new Parameters();
    p.set("filename", outputCorpus);
    if(corpusParameters.isLong("corpusBlockSize")){
      p.set("blockSize", corpusParameters.getLong("corpusBlockSize"));
    }
    if (p.isMap("corpusParameters")) {
      p.copyFrom(corpusParameters.getMap("corpusParameters"));
    }
    stage.add(new Step(CorpusFileWriter.class, p));

    job.add(stage);
    return job;
  }

  public Stage getParseWriteDocumentsStage(Parameters corpusParameters, Parameters corpusWriterParameters) {
    Stage stage = new Stage("parserWriter");
    stage.addInput("splits", new DocumentSplit.FileIdOrder());
    stage.addOutput("indexData", new KeyValuePair.KeyOrder());
    stage.add(new InputStep("splits"));
    stage.add(BuildStageTemplates.getParserStep(corpusParameters));
    stage.add(BuildStageTemplates.getTokenizerStep(corpusParameters));
    stage.add(new Step(DocumentNumberer.class));
    stage.add(new Step(CorpusFolderWriter.class, corpusWriterParameters.clone()));
    stage.add(Utility.getSorter(new KeyValuePair.KeyOrder()));
    stage.add(new OutputStep("indexData"));
    return stage;
  }

  public Stage getIndexWriterStage(Parameters corpusWriterParameters) {
    Stage stage = new Stage("indexWriter");

    stage.addInput("indexData", new KeyValuePair.KeyOrder());
    stage.add(new InputStep("indexData"));
    stage.add(new Step(SplitBTreeKeyWriter.class, corpusWriterParameters.clone()));

    return stage;
  }

  public Job getMakeCorpusJob(Parameters corpusParams) throws IOException {

    List<String> inputPaths = corpusParams.getAsList("inputPath");
    File corpus = new File(corpusParams.getString("corpusPath"));

    // check if we're creating a single file corpus
    if (corpusParams.get("corpusFormat", "folder").equals("file")) {
      return getCorpusFileJob(corpus.getAbsolutePath(), inputPaths, corpusParams);
    }

    // otherwise we're creating a folder corpus
    //  -> clear the directory.
    if (corpus.isDirectory()) {
      for (File f : corpus.listFiles()) {
        f.delete();
      }
    }

    Parameters corpusWriterParameters = new Parameters();
    corpusWriterParameters.set("readerClass", CorpusReader.class.getName());
    corpusWriterParameters.set("writerClass", CorpusFolderWriter.class.getName());
    corpusWriterParameters.set("filename", corpus.getAbsolutePath());
    // we need a small block size because the stored values are small
    corpusWriterParameters.set("blockSize", corpusParams.get("corpusBlockSize", 512));
    if (corpusParams.isMap("corpusParameters")) {
      corpusWriterParameters.copyFrom(corpusParams.getMap("corpusParameters"));
    }

    Job job = new Job();

    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", corpusParams.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));
    job.add(getParseWriteDocumentsStage(corpusParams, corpusWriterParameters));
    job.add(getIndexWriterStage(corpusWriterParameters));

    job.connect("inputSplit", "parserWriter", ConnectionAssignmentType.Each);
    job.connect("parserWriter", "indexWriter", ConnectionAssignmentType.Combined);

    return job;
  }
}
