// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.VByteInput;

/**
 * Retrieves lists of floating point numbers which can be used as document
 * features.
 * 
 * @author trevor, jfoley
 */
public final class SparseFloatListSource extends BTreeValueSource implements ScoreSource {
  /**
   * The score used for those values that were not included.
   */
  final double defaultScore;
  
  VByteInput stream;
  long indexCount;
  long index;
  long currentDocument;
  double currentScore;
  
  public SparseFloatListSource(BTreeIterator iter, double defaultScore) throws IOException {
    super(iter);
    this.defaultScore = defaultScore;
    reset();
  }
  
  @Override
  public void reset() throws IOException {
    DataStream buffered = btreeIter.getValueStream();
    stream = new VByteInput(buffered);
    indexCount = stream.readLong();
    index = -1;
    currentDocument = 0;
    if (indexCount > 0) {
      read();
    }
  }
  
  private void read() throws IOException {
      index += 1;

      if (index < indexCount) {
        currentDocument += stream.readLong();
        currentScore = stream.readFloat();
      } else {
        // ensure we never overflow
        index = indexCount;
      }
    }

  @Override
  public boolean isDone() {
    return index >= indexCount;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public long totalEntries() {
    return indexCount;
  }

  @Override
  public long currentCandidate() {
    return currentDocument;
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public void syncTo(long document) throws IOException {
    while (!isDone() && document > currentDocument) {
      read();
    }
  }

  @Override
  public double score(long id) {
    if (currentDocument == id) {
      return currentScore;
    }
    return defaultScore;
  }

  @Override
  public double maxScore() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double minScore() {
    return Double.NEGATIVE_INFINITY;
  }
  
}
