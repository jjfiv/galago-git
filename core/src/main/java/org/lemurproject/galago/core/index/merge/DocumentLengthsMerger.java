// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentLengthsMerger extends GenericIndexMerger<NumberedDocumentData> {

  public DocumentLengthsMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return true;
  }

  @Override
  public Processor<NumberedDocumentData> createIndexWriter(TupleFlowParameters parameters) throws IOException {
    return new DiskLengthsWriter(parameters);
  }
  
  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert( keyIterators.size() == 1 ) : "Found two identical keys when merging lengths. Length data should never be combined.";
    DiskLengthsReader.KeyIterator i = (DiskLengthsReader.KeyIterator) keyIterators.get(0).iterator;
    this.writer.process( new NumberedDocumentData(null, null, null, Utility.toInt(key), i.getCurrentLength()) );
  }
}
