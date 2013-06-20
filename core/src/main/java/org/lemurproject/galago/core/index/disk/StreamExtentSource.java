// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeValueSource;
import org.lemurproject.galago.core.index.ExtentSource;

import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author jfoley
 */
public class StreamExtentSource extends BTreeValueSource implements ExtentSource {
  int documentCount;
  int totalPositionCount;
  int maximumPositionCount;
  private VByteInput documents;
  private VByteInput counts;
  private VByteInput positions;
  private int documentIndex;
  private int currentDocument;
  private int currentCount;
  private boolean done;
  private ExtentArray extentArray;
  // to support resets
  protected long startPosition;
  protected long endPosition;
  // to support skipping
  private VByteInput skips;
  private VByteInput skipPositions;
  private DataStream skipPositionsStream;
  private DataStream documentsStream;
  private DataStream countsStream;
  private DataStream positionsStream;
  private int skipDistance;
  private int skipResetDistance;
  private long numSkips;
  private long skipsRead;
  private long nextSkipDocument;
  private long lastSkipPosition;
  private long documentsByteFloor;
  private long countsByteFloor;
  private long positionsByteFloor;
  // Supports lazy-loading of extents
  private boolean extentsLoaded;
  private int inlineMinimum;
  private int extentsByteSize;
  
  
  public StreamExtentSource(BTreeReader.BTreeIterator iter) throws IOException {
    super(iter);
  }
  
  @Override
  public void reset() throws IOException {
    extentArray = new ExtentArray();
    startPosition = btreeIter.getValueStart();
    endPosition = btreeIter.getValueEnd();
    currentDocument = 0;
    currentCount = 0;
    extentArray.reset();
    extentsLoaded = true;
    done = false;
    initialize();
  }
  
  /**
   * Initialization method.
   *
   * Even though we check for skips multiple times, in terms of how the data is loaded
   * its easier to do the parts when appropriate
   */
  private void initialize() throws IOException {
    DataStream valueStream = btreeIter.getSubValueStream(0, btreeIter.getValueLength());
    DataInput stream = new VByteInput(valueStream);
    // metadata
    int options = stream.readInt();
    if ((options & HAS_INLINING) == HAS_INLINING) {
      inlineMinimum = stream.readInt();
    } else {
      inlineMinimum = Integer.MAX_VALUE;
    }
    documentCount = stream.readInt();
    totalPositionCount = stream.readInt();
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
    positionsStream = btreeIter.getSubValueStream(positionsStart, positionsByteLength);
    documents = new VByteInput(documentsStream);
    counts = new VByteInput(countsStream);
    positions = new VByteInput(positionsStream);
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
      positionsByteFloor = 0;
    } else {
      assert positionsEnd == endPosition - startPosition;
      skips = null;
      skipPositions = null;
    }
    documentIndex = 0;
    extentsLoaded = true; // Not really, but this keeps it from reading ahead too soon.
    loadNextPosting();
  }
  
  private void loadNextPosting() throws IOException {
    if (documentIndex >= documentCount) {
      done = true;
      extentArray.reset();
      extentsLoaded = true;
      currentCount = 0;
      currentDocument = Integer.MAX_VALUE;
      return;
    }
    if (!extentsLoaded) {
      if (currentCount > inlineMinimum) {
        positions.skipBytes(extentsByteSize);
      } else {
        loadExtents();
      }
    }
    currentDocument += documents.readInt();
    currentCount = counts.readInt();
    // Prep the extents
    extentArray.reset();
    extentsLoaded = false;
    if (currentCount > inlineMinimum) {
      extentsByteSize = positions.readInt();
    } else {
      // Load them aggressively since we can't skip them
      loadExtents();
    }
  }
  
  /**
   * Loads up a single set of positions for an intID. Basically it's the
   * load that needs to be done when moving forward one in the posting list.
   */
  private void loadExtents() throws IOException {
    if (!extentsLoaded) {
      extentArray.setDocument(currentDocument);
      int position = 0;
      for (int i = 0; i < currentCount; i++) {
        position += positions.readInt();
        extentArray.add(position);
      }
      extentsLoaded = true;
    }
  }


  @Override
  public boolean isDone() {
    return this.done;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public long totalEntries() {
    return (long) documentCount;
  }

  @Override
  public int currentCandidate() {
    return currentDocument;
  }

  @Override
  public void movePast(int id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public void syncTo(int document) throws IOException {
     if (skips != null) {
      synchronizeSkipPositions();
    }
    if (skips != null && document > nextSkipDocument) {
      extentsLoaded = true;
      extentsByteSize = 0;
      // if we're here, we're skipping
      while (skipsRead < numSkips && document > nextSkipDocument) {
        skipOnce();
      }
      repositionMainStreams();
    }
    // Linear from here
    while (!isDone() && document > currentDocument) {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      if (!isDone()) {
        loadNextPosting();
      }
    }
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
  
  /** 
   * This only moves forward in tier 1, reads from tier 2 only when
   * needed to update floors
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
      positionsByteFloor = skipPositions.readLong();
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
  
  
  private void repositionMainStreams() throws IOException {
    // If we just reset the floors, don't read the 2nd tier again
    if ((skipsRead - 1) % skipResetDistance == 0) {
      documentsStream.seek(documentsByteFloor);
      countsStream.seek(countsByteFloor);
      positionsStream.seek(positionsByteFloor);
    } else {
      skipPositionsStream.seek(lastSkipPosition);
      documentsStream.seek(documentsByteFloor + skipPositions.readInt());
      countsStream.seek(countsByteFloor + skipPositions.readInt());
      positionsStream.seek(positionsByteFloor + skipPositions.readLong());
    }
    documentIndex = (int) (skipDistance * skipsRead) - 1;
  }


  @Override
  public ExtentArray extents(int id) {
    if (!done && id == this.currentCandidate()) {
      try {
        loadExtents();
        return extentArray;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return ExtentArray.EMPTY;
  }
  
  @Override
  public long count(int id) {
    if (!done && id == this.currentCandidate()) {
      return currentCount;
    }
    return 0;
  }
}

