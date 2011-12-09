// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentNameMerger extends GenericIndexMerger<NumberedDocumentData> {

  public DocumentNameMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return true;
  }
  
  @Override
  public Processor<NumberedDocumentData> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    return new DiskNameWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert (keyIterators.size() == 1) : "Found two identical keys when merging names. Name data should never be combined.";
    DiskNameReader.KeyIterator i = (DiskNameReader.KeyIterator) keyIterators.get(0).iterator;
    this.writer.process(new NumberedDocumentData(i.getCurrentName(),null,null, Utility.toInt(key), 0));
  }
}
