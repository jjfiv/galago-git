// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.btree.format.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * This iterator simply ignores the positions information - faster b/c when
 * incrementing or loading or skipping, we don't have to bookkeep the positions
 * buffer. Overall smaller footprint and faster execution.
 *
 * @author irmarc
 * @author jfoley
 * @see PositionIndexReader
 * @see StreamExtentSource
 */
final public class PositionIndexCountSource extends BTreeValueSource implements CountSource {

  public long documentCount;
  public long collectionCount;
  public long maximumPositionCount;
  VByteInput documents;
  VByteInput counts;
  long documentIndex;
  long currentDocument;
  int currentCount;
  boolean done;
  // Support for resets
  long startPosition;
  long endPosition;
  // to support skipping
  VByteInput skips;
  VByteInput skipPositions;
  DataStream skipPositionsStream;
  DataStream documentsStream;
  DataStream countsStream;
  long skipDistance;
  long skipResetDistance;
  long numSkips;
  long skipsRead;
  long nextSkipDocument;
  long lastSkipPosition;
  long documentsByteFloor;
  long countsByteFloor;

  public PositionIndexCountSource(BTreeReader.BTreeIterator iter) throws IOException {
    super(iter);
    reset();
  }

  public PositionIndexCountSource(BTreeReader.BTreeIterator iter, String dispKey) throws IOException {
    super(iter, dispKey);
    reset();
  }

  @Override
  public void reset() throws IOException {
    startPosition = btreeIter.getValueStart();
    endPosition = btreeIter.getValueEnd();
    currentDocument = 0;
    currentCount = 0;
    done = false;
    initialize();
  }

  /**
   * Initialization method.
   *
   * Even though we check for skips multiple times, in terms of how the data is
   * loaded its easier to do the parts when appropriate
   */
  protected void initialize() throws IOException {
    // all header information should be in the first 120 bytes
    DataStream valueStream = btreeIter.getSubValueStream(0, 120);
    DataInput stream = new VByteInput(valueStream);
    // metadata
    int options = stream.readInt(); // 5 bytes
    final boolean hasInlining = (options & HAS_INLINING) > 0;
    final boolean hasSkips = (options & HAS_SKIPS) > 0;
    final boolean hasMaxTF = (options & HAS_MAXTF) > 0;

    // Don't need to keep this value as positions are ignored.
    if ((options & HAS_INLINING) == HAS_INLINING) {
      int inlineMinimum = stream.readInt(); // 5 bytes
    }
    documentCount = stream.readLong(); // 9 bytes
    collectionCount = stream.readLong(); // 9 bytes
    if ((options & HAS_MAXTF) == HAS_MAXTF) {
      maximumPositionCount = stream.readLong(); // 5 bytes
    } else {
      maximumPositionCount = Long.MAX_VALUE;
    }
    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipDistance = stream.readLong(); // 9 bytes
      skipResetDistance = stream.readLong(); // 9 bytes
      numSkips = stream.readLong(); // 9 bytes
    }
    // segment lengths
    long documentByteLength = stream.readLong(); // 9 bytes
    long countsByteLength = stream.readLong(); // 9 bytes
    long positionsByteLength = stream.readLong(); // 9 bytes
    long skipsByteLength = 0;
    long skipPositionsByteLength = 0;
    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipsByteLength = stream.readLong(); // 9 bytes
      skipPositionsByteLength = stream.readLong(); // 9 bytes
    }

    // done with header (read at most (6 * 9) + (7 * 5) = 107 bytes)

    long documentStart = valueStream.getPosition();
    long countsStart = documentStart + documentByteLength;
    long positionsStart = countsStart + countsByteLength;
    long positionsEnd = positionsStart + positionsByteLength;

    documentsStream = btreeIter.getSubValueStream(documentStart, documentByteLength);
    countsStream = btreeIter.getSubValueStream(countsStart, countsByteLength);
    documents = new VByteInput(documentsStream);
    counts = new VByteInput(countsStream);

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      long skipsStart = positionsStart + positionsByteLength;
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
      // if we failed - give me a breakpoint.
      if (positionsEnd != (endPosition - startPosition)) {
        int i = 0;
      }
      assert positionsEnd == endPosition - startPosition;
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
    return false; //it's extremely unlikely
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
  public void movePast(long document) throws IOException {
    syncTo(document + 1);
  }

  @Override
  public void syncTo(long document) throws IOException {
    if (isDone()) {
      return;
    }

    if (!done && skips != null) {
      synchronizeSkipPositions();
      if (document > nextSkipDocument) {
        // if we're here, we're skipping
        while (!done && skipsRead < numSkips && document > nextSkipDocument) {
          skipOnce();
        }
        repositionMainStreams();
      }
    }
    // linear from here
    while (!isDone() && document > currentDocument) {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      load();
    }
  }

  /**
   * This only moves forward in tier 1, reads from tier 2 only when needed to
   * update floors
   *
   */
  private void skipOnce() throws IOException {
    // may have already skipped passed the final document
    if(nextSkipDocument == Long.MAX_VALUE){
      return;
    }

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
      documentsStream.seek(documentsByteFloor + skipPositions.readInt());
      countsStream.seek(countsByteFloor + skipPositions.readInt());
      // we seek here, so no reading needed
    }
    documentIndex = (int) (skipDistance * skipsRead) - 1;
  }

  @Override
  public int count(long id) {
    if (!done && currentCandidate() == id) {
      return currentCount;
    }
    return 0;
  }

  @Override
  public NodeStatistics getStatistics() {
    NodeStatistics ns = new NodeStatistics();
    ns.node = this.key();
    ns.maximumCount = this.maximumPositionCount;
    ns.nodeFrequency = this.collectionCount;
    ns.nodeDocumentCount = this.documentCount;
    return ns;
  }
}
