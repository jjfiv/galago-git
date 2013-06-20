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
  // final here to prevent reallocation of this during scoring
  final private ExtentArray extentArray;
  // to support resets
  final protected long startPosition;
  final protected long endPosition;
  
  private DataStream documentsStream;
  private DataStream countsStream;
  private DataStream positionsStream;
  
  private class SkipState {
    public VByteInput data;
    public VByteInput positions;
    public DataStream positionsStream;

    public int distance;
    public int resetDistance;
    public long total;
    public long read;
    public long nextDocument;
    public long nextPosition;
  }
  // to support skipping
  private SkipState skip;
  
  private long documentsByteFloor;
  private long countsByteFloor;
  private long positionsByteFloor;
  // Supports lazy-loading of extents
  private boolean extentsLoaded;
  private int inlineMinimum;
  private int extentsByteSize;
  
  
  public StreamExtentSource(BTreeReader.BTreeIterator iter) throws IOException {
    super(iter);
    startPosition = btreeIter.getValueStart();
    endPosition = btreeIter.getValueEnd();
    extentArray = new ExtentArray();
    initialize();
  }
  
  @Override
  public void reset() throws IOException {
    currentDocument = 0;
    currentCount = 0;
    extentArray.reset();
    extentsLoaded = true;
    done = false;
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
      skip = new SkipState();
      skip.distance = stream.readInt();
      skip.resetDistance = stream.readInt();
      skip.total = stream.readLong();
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
      skip.data = new VByteInput(btreeIter.getSubValueStream(skipsStart, skipsByteLength));
      skip.positionsStream = btreeIter.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
      skip.positions = new VByteInput(skipPositionsStream);
      // load up
      skip.nextDocument = skip.data.readInt();
      documentsByteFloor = 0;
      countsByteFloor = 0;
      positionsByteFloor = 0;
    } else {
      assert positionsEnd == endPosition - startPosition;
      skip = null;
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
     if (skip != null) {
      synchronizeSkipPositions();
    }
    if (skip != null && document > skip.nextDocument) {
      extentsLoaded = true;
      extentsByteSize = 0; 
      // if we're here, we're skipping
      while (skip.read < skip.total && document > skip.nextDocument) {
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
    while (skip.nextDocument <= currentDocument) {
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
    assert skip.read < skip.total;
    long currentSkipPosition = skip.nextPosition + skip.data.readInt();
    if (skip.read % skip.resetDistance == 0) {
      // Position the skip positions stream
      skip.positionsStream.seek(currentSkipPosition);
      // now set the floor values
      documentsByteFloor = skip.positions.readInt();
      countsByteFloor = skip.positions.readInt();
      positionsByteFloor = skip.positions.readLong();
    }
    currentDocument = (int) skip.nextDocument;
    // May be at the end of the buffer
    if (skip.read + 1 == skip.total) {
      skip.nextDocument = Integer.MAX_VALUE;
    } else {
      skip.nextDocument += skip.data.readInt();
    }
    skip.read++;
    skip.nextPosition = currentSkipPosition;
  }
  
  
  private void repositionMainStreams() throws IOException {
    // If we just reset the floors, don't read the 2nd tier again
    if ((skip.read - 1) % skip.resetDistance == 0) {
      documentsStream.seek(documentsByteFloor);
      countsStream.seek(countsByteFloor);
      positionsStream.seek(positionsByteFloor);
    } else {
      skip.positionsStream.seek(skip.nextPosition);
      documentsStream.seek(documentsByteFloor + skip.positions.readInt());
      countsStream.seek(countsByteFloor + skip.positions.readInt());
      positionsStream.seek(positionsByteFloor + skip.positions.readLong());
    }
    documentIndex = (int) (skip.distance * skip.read) - 1;
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

