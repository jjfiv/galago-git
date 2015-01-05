// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.VByteInput;

import java.io.DataInput;
import java.io.IOException;

/**
 * Reads a simple positions-based index, where each inverted list in the index
 * contains both term count information and term position information. The term
 * counts data is stored separately from term position information for faster
 * query processing when no positions are needed.
 *
 * @author sjh, irmarc, jfoley
 */
public final class WindowIndexCountSource extends BTreeValueSource implements CountSource {

  long documentCount;
  long collectionCount;
  VByteInput documents;
  VByteInput counts;
  long documentIndex;
  long currentDocument;
  int currentCount;
  boolean done;
  long maximumPositionCount;
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

  public WindowIndexCountSource(BTreeIterator iter) throws IOException {
    super(iter);
    reset();
  }

  /**
   * Initialization/reset method.
   *
   * Even though we check for skips multiple times, in terms of how the data is
   * loaded its easier to do the parts when appropriate
   */
  @Override
  public void reset() throws IOException {
    // need to read at most (15 + 90) bytes //
    DataStream valueStream = btreeIter.getSubValueStream(0, 120);
    DataInput stream = new VByteInput(valueStream);

    // metadata
    int options = stream.readInt(); // 5 bytes
    documentCount = stream.readLong(); // 9 bytes
    collectionCount = stream.readLong(); // 9 bytes

    if ((options & HAS_MAXTF) == HAS_MAXTF) {
      maximumPositionCount = stream.readLong(); // 9 bytes
    } else {
      maximumPositionCount = Integer.MAX_VALUE;
    }

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipDistance = stream.readInt(); // 5 bytes
      skipResetDistance = stream.readInt(); // 5 bytes
      numSkips = stream.readLong(); // 9 bytes
    }

    // segment lengths
    long documentByteLength = stream.readLong(); // 9 bytes
    long countsByteLength = stream.readLong(); // 9 bytes
    long beginsByteLength = stream.readLong(); // 9 bytes
    long endsByteLength = stream.readLong(); // 9 bytes
    long skipsByteLength = 0;
    long skipPositionsByteLength = 0;

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipsByteLength = stream.readLong(); // 9 bytes
      skipPositionsByteLength = stream.readLong(); // 9 bytes
    }

    long documentStart = valueStream.getPosition();
    long countsStart = documentStart + documentByteLength;
    long beginsStart = countsStart + countsByteLength;
    long endsStart = beginsStart + beginsByteLength;
    long endsEnd = endsStart + endsByteLength;

    documentsStream = btreeIter.getSubValueStream(documentStart, documentByteLength);
    countsStream = btreeIter.getSubValueStream(countsStart, countsByteLength);


    documents = new VByteInput(documentsStream);
    counts = new VByteInput(countsStream);

    if ((options & HAS_SKIPS) == HAS_SKIPS) {

      long skipsStart = endsStart + endsByteLength;
      long skipPositionsStart = skipsStart + skipsByteLength;
      long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

      assert skipPositionsEnd == btreeIter.getValueLength();

      skips = new VByteInput(btreeIter.getSubValueStream(skipsStart, skipsByteLength));
      skipPositionsStream = btreeIter.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
      skipPositions = new VByteInput(skipPositionsStream);

      // load up
      nextSkipDocument = skips.readLong();
      documentsByteFloor = 0;
      countsByteFloor = 0;
    } else {
      assert endsEnd == btreeIter.getValueLength();
      skips = null;
      skipPositions = null;
    }

    documentIndex = 0;
    load();
  }

  // Only loading the docid and the count
  private void load() throws IOException {
    if (documentIndex >= documentCount) {
      done = true;
      currentDocument = Long.MAX_VALUE;
      currentCount = 0;
      return;
    }
    currentDocument += documents.readLong();
    currentCount = counts.readInt();
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public boolean hasAllCandidates() {
    return done;
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
    // can shortcut the skip code.
    if (done) {
      return;
    }

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
    while (!done && document > currentDocument) {
      documentIndex += 1;
      load();
    }
  }

  /**
   * This only moves forward in tier 1, reads from tier 2 only when needed to
   * update floors.
   */
  private void skipOnce() throws IOException {
    assert skipsRead < numSkips;
    long currentSkipPosition = lastSkipPosition + skips.readLong();

    if (skipsRead % skipResetDistance == 0) {
      // Position the skip positions stream
      skipPositionsStream.seek(currentSkipPosition);

      // now set the floor values
      documentsByteFloor = skipPositions.readLong();
      countsByteFloor = skipPositions.readLong();
    }
    currentDocument = nextSkipDocument;

    // May be at the end of the buffer
    if (skipsRead + 1 == numSkips) {
      nextSkipDocument = Long.MAX_VALUE;
    } else {
      nextSkipDocument += skips.readLong();
    }
    skipsRead++;
    lastSkipPosition = currentSkipPosition;
  }

  /**
   * This makes sure the skip list pointers are still ahead of the current
   * document. If we called "next" a lot, these may be out of sync.
   */
  private void synchronizeSkipPositions() throws IOException {
    while (!done && nextSkipDocument <= currentDocument) {
      long cd = currentDocument;
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
      documentsStream.seek(documentsByteFloor + skipPositions.readLong());
      countsStream.seek(countsByteFloor + skipPositions.readLong());
      // we seek here, so no reading needed
    }
    documentIndex = (skipDistance * skipsRead) - 1;
  }

  @Override
  public int count(long id) {
    if (!done && id == currentCandidate()) {
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
