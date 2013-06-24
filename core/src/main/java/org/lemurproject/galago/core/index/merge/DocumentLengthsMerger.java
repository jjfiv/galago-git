// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.types.FieldLengthData;
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
    return false; // keys are not mappable
  }

  @Override
  public Processor<FieldLengthData> createIndexWriter(TupleFlowParameters parameters) throws IOException {
    return new DiskLengthsWriter(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    PriorityQueue<LengthIteratorWrapper> lenQueue = new PriorityQueue();
    for (KeyIteratorWrapper wrapper : keyIterators) {
      lenQueue.offer(new LengthIteratorWrapper(this.partIds.get(wrapper), (LengthsIterator) wrapper.getIterator().getValueIterator(), this.mappingReader));
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
    ScoringContext sc;
    LengthsIterator iterator;
    long currentDocument;
    int currentLength;
    DocumentMappingReader mapping;

    private LengthIteratorWrapper(int indexId, LengthsIterator iterator, DocumentMappingReader mapping) {
      this.indexId = indexId;
      this.iterator = iterator;
      this.mapping = mapping;

      this.sc = new ScoringContext();
      this.iterator.setContext(sc);
      
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
      long currentIdentifier = iterator.currentCandidate();
      sc.document = currentIdentifier;
      this.currentDocument = mapping.map(indexId, currentIdentifier);
      this.currentLength = iterator.length();
    }

    public int getLength() {
      return iterator.length();
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    @Override
    public int compareTo(LengthIteratorWrapper other) {
      return Utility.compare(currentDocument, other.currentDocument);
    }
  }
}
