/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.utility.CmpUtil;

/**
 *
 * @author sjh
 */
public abstract class GenericExtentValueIndexMerger<S> extends GenericIndexMerger<S> {

  // wrapper class for ExtentValueIterators
  private class ExtentValueIteratorWrapper implements Comparable<ExtentValueIteratorWrapper> {

    ScoringContext sc = new ScoringContext();
    int indexId;
    ExtentIterator iterator;
    long currentDocument;
    ExtentArray currentExtentArray;
    DocumentMappingReader mapping;

    private ExtentValueIteratorWrapper(int indexId, ExtentIterator extentIterator, DocumentMappingReader mapping) {
      this.indexId = indexId;
      this.iterator = extentIterator;
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
      this.sc.document = iterator.currentCandidate();
      this.currentExtentArray = iterator.extents(sc);
      this.currentDocument = mapping.map(indexId, currentExtentArray.getDocument());
      this.currentExtentArray.setDocument(this.currentDocument);
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public int compareTo(ExtentValueIteratorWrapper other) {
      return CmpUtil.compare(currentDocument, other.currentDocument);
    }
  }

  // overridden functions
  public GenericExtentValueIndexMerger(TupleFlowParameters parameters) throws Exception {
    super(parameters);
  }

  @Override
  public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {
    PriorityQueue<ExtentValueIteratorWrapper> extentQueue = new PriorityQueue<>();
    for (KeyIteratorWrapper w : keyIterators) {
      ExtentIterator extentIterator = (ExtentIterator) w.iterator.getValueIterator();
      extentQueue.add(new ExtentValueIteratorWrapper(this.partIds.get(w), extentIterator, this.mappingReader));
    }

    while (!extentQueue.isEmpty()) {
      ExtentValueIteratorWrapper head = extentQueue.poll();
      transformExtentArray(key, head.currentExtentArray);
      head.next();
      if (!head.isDone()) {
        extentQueue.offer(head);
      }
    }
  }

  public abstract void transformExtentArray(byte[] key, ExtentArray extentArray) throws IOException;
}
