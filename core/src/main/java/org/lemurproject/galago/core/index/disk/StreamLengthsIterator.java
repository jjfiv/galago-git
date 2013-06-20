// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.CollectionStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * StreamLengthsIterator is an iterator used by the DiskLengthsReader
 * @author jfoley
 */
public class StreamLengthsIterator extends BTreeValueIterator implements CountIterator, LengthsIterator, CollectionAggregateIterator {
  final BTreeReader.BTreeIterator iterator;
  DataStream streamBuffer;
  // stats
  long totalDocumentCount;
  long nonZeroDocumentCount;
  long collectionLength;
  double avgLength;
  long maxLength;
  long minLength;
  // utility
  int firstDocument;
  int lastDocument;
  // iteration vars
  int currDocument;
  int currLength;
  long lengthsDataOffset;
  boolean done;

  public StreamLengthsIterator(byte[] key, BTreeReader.BTreeIterator it) throws IOException {
    super(key);
    this.iterator = it;
    reset(it);
  }

  @Override
  public void reset(BTreeReader.BTreeIterator it) throws IOException {
    this.streamBuffer = it.getValueStream();
    BTreeReader reader = it.reader;
    // collect stats
    //** temporary fix - this allows current indexes to continue to work **/
    if (reader.getManifest().get("version", 1) == 3) {
      this.totalDocumentCount = streamBuffer.readLong();
      this.nonZeroDocumentCount = streamBuffer.readLong();
      this.collectionLength = streamBuffer.readLong();
      this.avgLength = streamBuffer.readDouble();
      this.maxLength = streamBuffer.readLong();
      this.minLength = streamBuffer.readLong();
    } else if (reader.getManifest().get("longs", false)) {
      this.nonZeroDocumentCount = streamBuffer.readLong();
      this.collectionLength = streamBuffer.readLong();
      this.avgLength = streamBuffer.readDouble();
      this.maxLength = streamBuffer.readLong();
      this.minLength = streamBuffer.readLong();
      this.totalDocumentCount = this.nonZeroDocumentCount;
    } else {
      this.nonZeroDocumentCount = streamBuffer.readInt();
      this.collectionLength = streamBuffer.readInt();
      this.avgLength = streamBuffer.readDouble();
      this.maxLength = streamBuffer.readInt();
      this.minLength = streamBuffer.readInt();
      this.totalDocumentCount = this.nonZeroDocumentCount;
    }
    this.firstDocument = streamBuffer.readInt();
    this.lastDocument = streamBuffer.readInt();
    this.lengthsDataOffset = this.streamBuffer.getPosition(); // should be == (4 * 6) + (8)
    // offset is the first document
    this.currDocument = firstDocument;
    this.currLength = -1;
    this.done = (currDocument > lastDocument);
  }

  @Override
  public void reset() throws IOException {
    this.reset(iterator);
  }

  @Override
  public int currentCandidate() {
    return this.currDocument;
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public void syncTo(int identifier) throws IOException {
    // it's possible that the first document has zero length, and we may wish to sync to it.
    if (identifier < firstDocument) {
      return;
    }
    assert (identifier >= currDocument) : "StreamLengthsIterator reader can't move to a previous document.";
    // we can't move past the last document
    if (identifier > lastDocument) {
      done = true;
      identifier = lastDocument;
    }
    if (currDocument < identifier) {
      // we only delete the length if we move
      // this is because we can't re-read the length value
      currDocument = identifier;
      currLength = -1;
    }
  }

  @Override
  public void movePast(int identifier) throws IOException {
    // select the next document:
    identifier += 1;
    assert (identifier >= currDocument);
    // we can't move past the last document
    if (identifier > lastDocument) {
      done = true;
      identifier = lastDocument;
    }
    if (currDocument < identifier) {
      // we only delete the length if we move
      // this is because we can't re-read the length value
      currDocument = identifier;
      currLength = -1;
    }
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public String getValueString() throws IOException {
    return currentCandidate() + "," + getCurrentLength();
  }

  @Override
  public long totalEntries() {
    return this.totalDocumentCount;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "lengths";
    String className = this.getClass().getSimpleName();
    String parameters = Utility.toString(key);
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(getCurrentLength());
    List<AnnotatedNode> children = Collections.EMPTY_LIST;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public int count() {
    return getCurrentLength();
  }

  @Override
  public int getCurrentLength() {
    if (context.document == this.currDocument) {
      // check if we need to read the length value from the stream
      if (this.currLength < 0) {
        // ensure a defaulty value
        this.currLength = 0;
        // check for range.
        if (firstDocument <= currDocument && currDocument <= lastDocument) {
          // seek to the required position - hopefully this will hit cache
          this.streamBuffer.seek(lengthsDataOffset + (4 * (this.currDocument - firstDocument)));
          try {
            this.currLength = this.streamBuffer.readInt();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      return currLength;
    } else {
      return 0;
    }
  }

  @Override
  public int maximumCount() {
    return Integer.MAX_VALUE;
  }

  @Override
  public CollectionStatistics getStatistics() {
    CollectionStatistics cs = new CollectionStatistics();
    cs.fieldName = Utility.toString(key);
    cs.collectionLength = this.collectionLength;
    cs.documentCount = this.totalDocumentCount;
    cs.nonZeroLenDocCount = this.nonZeroDocumentCount;
    cs.maxLength = this.maxLength;
    cs.minLength = this.minLength;
    cs.avgLength = this.avgLength;
    return cs;
  }
  
}
