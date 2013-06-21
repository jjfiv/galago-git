// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * This iterator simply ignores the positions information - faster b/c when
 * incrementing or loading or skipping, we don't have to bookkeep the
 * positions buffer. Overall smaller footprint and faster execution.
 * @author irmarc
 * @author jfoley
 * @see PositionIndexReader
 * @see StreamExtentSource
 */
final public class PositionIndexCountSource extends BTreeValueSource implements CountSource {
  public int documentCount;
  public int collectionCount;
  public int maximumPositionCount;
  
  VByteInput documents;
  VByteInput counts;
  int documentIndex;
  int currentDocument;
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
  int skipDistance;
  int skipResetDistance;
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
   * Even though we check for skips multiple times, in terms of how the data is loaded
   * its easier to do the parts when appropriate
   */
  protected void initialize() throws IOException {
    DataStream valueStream = btreeIter.getSubValueStream(0, btreeIter.getValueLength());
    DataInput stream = new VByteInput(valueStream);
    // metadata
    int options = stream.readInt();
    final boolean hasInlining = (options & HAS_INLINING) > 0;
    final boolean hasSkips = (options & HAS_SKIPS) > 0;
    final boolean hasMaxTF = (options & HAS_MAXTF) > 0;
    
    // Don't need to keep this value as positions are ignored.
    if ((options & HAS_INLINING) == HAS_INLINING) {
      int inlineMinimum = stream.readInt();
    }
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
    long positionsByteLength = stream.readLong();
    long skipsByteLength = 0;
    long skipPositionsByteLength = 0;
    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipsByteLength = stream.readLong();
      skipPositionsByteLength = stream.readLong();
    }
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
      currentDocument = Integer.MAX_VALUE;
      currentCount = 0;
      return;
    }
    currentDocument += documents.readInt();
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
  public int currentCandidate() {
    return currentDocument;
  }

  @Override
  public void movePast(int document) throws IOException {
    syncTo(document+1);
  }

  @Override
  public void syncTo(int document) throws IOException {
    if (done) return;
    
    if (skips != null) {
      synchronizeSkipPositions();
      if (document > nextSkipDocument) {
        // if we're here, we're skipping
        while (skipsRead < numSkips && document > nextSkipDocument) {
          skipOnce();
        }
        repositionMainStreams();
      }
    }
    // linear from here
    while (!done && document > currentDocument) {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      load();
    }
  }
  
  /**
   * This only moves forward in tier 1, reads from tier 2 only when
   * needed to update floors
   *
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
   * This makes sure the skip list pointers are still ahead of the current document.
   * If we called "next" a lot, these may be out of sync.
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
  public long count(int id) {
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
