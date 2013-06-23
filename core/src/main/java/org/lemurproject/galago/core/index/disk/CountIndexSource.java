// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author jfoley
 */
public class CountIndexSource extends BTreeValueSource implements CountSource {
  int documentCount;
  int collectionCount;
  int maximumPositionCount;
  VByteInput documents;
  VByteInput counts;
  int documentIndex;
  int currentDocument;
  int currentCount;
  // Support for resets
  final long startPosition, endPosition;
  // to support skipping
  VByteInput skips;
  VByteInput skipPositions;
  DataStream skipPositionsStream;
  DataStream documentsStream;
  DataStream countsStream;
  int skipDistance;
  int skipResetDistance;
  long numSkips;
  long skipsRead;
  long nextSkipDocument;
  long lastSkipPosition;
  long documentsByteFloor;
  long countsByteFloor;

  public CountIndexSource(BTreeIterator it) throws IOException {
    super(it);
    startPosition = btreeIter.getValueStart();
    endPosition = btreeIter.getValueEnd();
    reset();
  }

  @Override
  public void reset() throws IOException {
    currentDocument = 0;
    currentCount = 0;
    initialize();
  }

  /**
   * Initialization method.
   *
   * Even though we check for skips multiple times, in terms of how the data is
   * loaded its easier to do the parts when appropriate
   */
  protected void initialize() throws IOException {
    DataStream valueStream = btreeIter.getSubValueStream(0, btreeIter.getValueLength());
    DataInput stream = new VByteInput(valueStream);

    // metadata
    int options = stream.readInt();
    documentCount = stream.readInt();
    collectionCount = stream.readInt();

    if ((options & HAS_MAXTF) == HAS_MAXTF) {
      maximumPositionCount = stream.readInt();
    } else {
      maximumPositionCount = Integer.MAX_VALUE;
    }

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipDistance = stream.readInt();
      skipResetDistance = stream.readInt();
      numSkips = stream.readLong();
    }

    // segment lengths
    long documentByteLength = stream.readLong();
    long countsByteLength = stream.readLong();
    long skipsByteLength = 0;
    long skipPositionsByteLength = 0;

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipsByteLength = stream.readLong();
      skipPositionsByteLength = stream.readLong();
    }

    long documentStart = valueStream.getPosition();
    long countsStart = documentStart + documentByteLength;
    long countsEnd = countsStart + countsByteLength;

    documentsStream = btreeIter.getSubValueStream(documentStart, documentByteLength);
    countsStream = btreeIter.getSubValueStream(countsStart, countsByteLength);

    documents = new VByteInput(documentsStream);
    counts = new VByteInput(countsStream);

    if ((options & HAS_SKIPS) == HAS_SKIPS) {

      long skipsStart = countsEnd;
      long skipPositionsStart = skipsStart + skipsByteLength;
      long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

      assert skipPositionsEnd == endPosition - startPosition;

      skips = new VByteInput(btreeIter.getSubValueStream(skipsStart, skipsByteLength));
      skipPositionsStream = btreeIter.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
      skipPositions = new VByteInput(skipPositionsStream);

      // load up
      nextSkipDocument = skips.readInt();
      documentsByteFloor = 0;
      countsByteFloor = 0;
    } else {
      assert countsEnd == endPosition - startPosition;
      skips = null;
      skipPositions = null;
    }

    documentIndex = 0;
    load();
  }

  /**
   * Only loading the docid and the count
   */
  private void load() throws IOException {
    currentDocument += documents.readInt();
    currentCount = counts.readInt();
  }

  @Override
  public boolean isDone() {
    return documentIndex >= documentCount;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public long totalEntries() {
    return documentCount;
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
    if (skips != null) {
      synchronizeSkipPositions();
      if (document > nextSkipDocument) {
        // if we're here, we're skipping
        while (skipsRead < numSkips
                && document > nextSkipDocument) {
          skipOnce();
        }
        repositionMainStreams();
      }
    }


    // linear from here
    while (!isDone() && document > currentDocument) {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      if (!isDone()) {
        load();
      }
    }
  }

  /**
   * This only moves forward in tier 1, reads from tier 2 only when needed to
   * update floors
   */
  private void skipOnce() throws IOException {
    assert skipsRead < numSkips;
    long currentSkipPosition = lastSkipPosition + skips.readInt();

    if (skipsRead % skipResetDistance == 0) {
      // Position the skip positions stream
      skipPositionsStream.seek(currentSkipPosition);

      // now set the floor values
      documentsByteFloor = skipPositions.readInt();
      countsByteFloor = skipPositions.readInt();
    }
    currentDocument = (int) nextSkipDocument;

    // May be at the end of the buffer
    if (skipsRead + 1 == numSkips) {
      nextSkipDocument = Integer.MAX_VALUE;
    } else {
      nextSkipDocument += skips.readInt();
    }
    skipsRead++;
    lastSkipPosition = currentSkipPosition;
  }

  /**
   * This makes sure the skip list pointers are still ahead of the current
   * document. If we called "next" a lot, these may be out of sync.
   */
  private void synchronizeSkipPositions() throws IOException {
    while (nextSkipDocument <= currentDocument) {
      int cd = currentDocument;
      skipOnce();
      currentDocument = cd;
    }
  }

  private void repositionMainStreams() throws IOException {
    // If we just reset the floors, don't read the 2nd tier again
    if ((skipsRead - 1) % skipResetDistance == 0) {
      documentsStream.seek(documentsByteFloor);
      countsStream.seek(countsByteFloor);
    } else {
      skipPositionsStream.seek(lastSkipPosition);
      documentsStream.seek(documentsByteFloor + skipPositions.readInt());
      countsStream.seek(countsByteFloor + skipPositions.readInt());
      // we seek here, so no reading needed
    }
    documentIndex = (int) (skipDistance * skipsRead) - 1;
  }

  @Override
  public int count(long id) {
    if (currentDocument == id) {
      return currentCount;
    }
    return 0;
  }

  @Override
  public NodeStatistics getStatistics() {
    NodeStatistics stats = new NodeStatistics();
    stats.node = this.key();
    stats.nodeFrequency = this.collectionCount;
    stats.nodeDocumentCount = this.documentCount;
    stats.maximumCount = this.maximumPositionCount;
    return stats;
  }
}
