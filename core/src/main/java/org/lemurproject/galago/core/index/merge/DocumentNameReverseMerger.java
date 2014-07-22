// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.types.DocumentNameId;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.IOException;
import java.util.List;

/**
 *
 * @author sjh
 */
public class DocumentNameReverseMerger extends GenericIndexMerger<DocumentNameId> {

  public DocumentNameReverseMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return false;
  }

  @Override
  public Processor<DocumentNameId> createIndexWriter(TupleFlowParameters parameters) throws Exception {
    return new DiskNameReverseWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    assert (keyIterators.size() == 1) : "Found two identical keys when merging names. Name data should never be combined.";
    DiskNameReverseReader.KeyIterator i = (DiskNameReverseReader.KeyIterator) keyIterators.get(0).iterator;
    long documentId = this.mappingReader.map(this.partIds.get(keyIterators.get(0)), i.getCurrentIdentifier());
    this.writer.process(new DocumentNameId(ByteUtil.fromString(i.getCurrentName()), documentId));
  }
}
