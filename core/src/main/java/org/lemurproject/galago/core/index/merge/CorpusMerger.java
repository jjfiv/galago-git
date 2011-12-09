// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.List;

import org.lemurproject.galago.core.index.corpus.CorpusFileWriter;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

/**
 *
 * @author sjh
 */
public class CorpusMerger extends GenericIndexMerger<Document> {

  public CorpusMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return true;
  }

  @Override
  public Processor<Document> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    return new CorpusFileWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert (keyIterators.size() == 1) : "Found two identical keys when merging names. Documents can never be combined.";
    Document d = ((DocumentReader.DocumentIterator) keyIterators.get(0).iterator).getDocument() ;
    this.writer.process(d);
  }
}
