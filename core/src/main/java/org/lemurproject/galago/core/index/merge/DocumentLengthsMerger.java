// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentLengthsMerger extends GenericIndexMerger<FieldLengthData> {

  public DocumentLengthsMerger(TupleFlowParameters p) throws Exception {
    super(p);
  }

  @Override
  public boolean mappingKeys() {
    return false; // keys are no longer mappable
  }

  @Override
  public Processor<FieldLengthData> createIndexWriter(TupleFlowParameters parameters) throws IOException {
    return new DiskLengthsWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    PriorityQueue<LengthIteratorWrapper> lenQueue = new PriorityQueue();
    for (KeyIteratorWrapper wrapper : keyIterators) {
      lenQueue.offer(new LengthIteratorWrapper(this.partIds.get(wrapper), (LengthsReader.LengthsIterator) wrapper.getIterator().getValueIterator(), this.mappingReader));
    }

    while (!lenQueue.isEmpty()) {
      LengthIteratorWrapper head = lenQueue.poll();
      while (!head.isDone()) {
        this.writer.process(new FieldLengthData(key, head.currentDocument, head.currentLength));
        head.next();
      }
    }
  }

  private class LengthIteratorWrapper implements Comparable<LengthIteratorWrapper> {

    int indexId;
    LengthsReader.LengthsIterator iterator;
    int currentDocument;
    int currentLength;
    DocumentMappingReader mapping;

    private LengthIteratorWrapper(int indexId, LengthsReader.LengthsIterator iterator, DocumentMappingReader mapping) {
      this.indexId = indexId;
      this.iterator = iterator;
      this.mapping = mapping;

      // initialization
      load();
    }

    public void next() throws IOException {
      iterator.movePast(iterator.currentCandidate());
      if (!iterator.isDone()) {
        load();
      }
    }

    // changes the document numbers in the extent array
    private void load() {
      int currentIdentifier = iterator.getCurrentIdentifier();
      this.currentDocument = mapping.map(indexId, currentIdentifier);
      this.currentLength = iterator.getCurrentLength();
    }

    public int getLength() {
      return iterator.getCurrentLength();
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public int compareTo(LengthIteratorWrapper other) {
      return Utility.compare(currentDocument, other.currentDocument);
    }
  }
}
