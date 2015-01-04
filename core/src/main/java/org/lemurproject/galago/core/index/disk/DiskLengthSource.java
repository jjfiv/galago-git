// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;

import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.LengthSource;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.utility.buffer.DataStream;

/**
 *
 * @author jfoley
 */
final public class DiskLengthSource extends BTreeValueSource implements LengthSource {

  DataStream streamBuffer;
  // stats
  public long totalDocumentCount;
  public long nonZeroDocumentCount;
  public long collectionLength;
  public double avgLength;
  public long maxLength;
  public long minLength;
  // utility
  long firstDocument;
  long lastDocument;
  // iteration vars
  long currDocument;
  int currLength;
  long lengthsDataOffset;
  boolean done;

  public DiskLengthSource(BTreeIterator iter) throws IOException {
    super(iter);
    reset();
  }

  @Override
  public void reset() throws IOException {
    this.streamBuffer = btreeIter.getValueStream();
    // collect stats
    //** temporary fix - this allows current indexes to continue to work **/
    this.totalDocumentCount = streamBuffer.readLong();
    this.nonZeroDocumentCount = streamBuffer.readLong();
    this.collectionLength = streamBuffer.readLong();
    this.avgLength = streamBuffer.readDouble();
    this.maxLength = streamBuffer.readLong();
    this.minLength = streamBuffer.readLong();
    this.firstDocument = streamBuffer.readLong();
    this.lastDocument = streamBuffer.readLong();
    this.lengthsDataOffset = this.streamBuffer.getPosition(); // should be == (4 * 6) + (8)
    // offset is the first document
    this.currDocument = firstDocument;
    this.currLength = -1;
    this.done = (currDocument > lastDocument);
  }

  @Override
  public boolean isDone() {
    return this.done;
  }

  @Override
  public long currentCandidate() {
    return this.currDocument;
  }

  @Override
  public void movePast(long identifier) {
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
  public int length(long document) {
    if (document == this.currDocument) {
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
  public void syncTo(long identifier) {
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
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public long totalEntries() {
    return this.totalDocumentCount;
  }

  @Override
  public FieldStatistics getStatistics() {
    FieldStatistics fs = new FieldStatistics();
    fs.fieldName = this.key();
    fs.collectionLength = this.collectionLength;
    fs.documentCount = this.totalDocumentCount;
    fs.nonZeroLenDocCount = this.nonZeroDocumentCount;
    fs.maxLength = this.maxLength;
    fs.minLength = this.minLength;
    fs.avgLength = this.avgLength;
    fs.firstDocId = this.firstDocument;
    fs.lastDocId = this.lastDocument;
    return fs;
  }

}
