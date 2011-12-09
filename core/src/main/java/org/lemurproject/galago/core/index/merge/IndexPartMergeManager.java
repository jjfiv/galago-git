/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
public class IndexPartMergeManager implements Processor<DocumentSplit> {

  TupleFlowParameters parameters;
  CollectionStatistics partStats = null;
  String part;
  HashMap<IndexPartReader, Integer> indexPartReaders = new HashMap();
  String mergerClassName = null;
  String writerClassName = null;
  DocumentMappingReader mappingData = null;

  public IndexPartMergeManager(TupleFlowParameters parameters) throws IOException {
    this.parameters = parameters;
    part = parameters.getJSON().getString("part");

    if (parameters.getJSON().containsKey("mappingDataStream")) {
      String mappingDataStreamName = parameters.getJSON().get("mappingDataStream", "");
      TypeReader mappingDataStream = parameters.getTypeReader(mappingDataStreamName);
      mappingData = new DocumentMappingReader(mappingDataStream);
    }
  }

  public void process(DocumentSplit index) throws IOException {
    IndexPartReader reader = DiskIndex.openIndexPart(index.fileName + File.separator + part);
    
    // do not worry about empty files
    if(reader.getManifest().get("emptyIndexFile", false)){
      return;
    }

    mergeStatistics(reader.getManifest());


    if (mergerClassName == null) {
      mergerClassName = reader.getManifest().getString("mergerClass");
      writerClassName = reader.getManifest().getString("writerClass");
    } else {
      assert (mergerClassName.equals(reader.getManifest().getString("mergerClass"))) : "mergeClass attributes are inconsistent.";
      assert (writerClassName.equals(reader.getManifest().getString("writerClass"))) : "writerClass attributes are inconsistent.";
    }

    indexPartReaders.put(reader, index.fileId);
  }

  public void close() throws IOException {
    if(indexPartReaders.isEmpty()){
      return;
    }
    
    try {
      parameters.getJSON().set("writerClass", writerClassName);
      parameters.getJSON().copyFrom( partStats.toParameters() );
      Class m = Class.forName(mergerClassName);
      Constructor c = m.getConstructor(TupleFlowParameters.class);

      GenericIndexMerger merger = (GenericIndexMerger) c.newInstance(parameters);

      merger.setDocumentMapping(mappingData);
      merger.setInputs(indexPartReaders);
      merger.performKeyMerge();
      merger.close();

    } catch (Exception ex) {
      Logger.getLogger(IndexPartMergeManager.class.getName()).log(Level.SEVERE, "Errored Merging Part: " + part, ex);
      throw new IOException(ex);
    }
  }

  private void mergeStatistics(Parameters manifest) {
    if (partStats == null) {
      partStats = new CollectionStatistics(part, manifest);
    } else {
      partStats.add(new CollectionStatistics(part, manifest));
    }
  }
}
