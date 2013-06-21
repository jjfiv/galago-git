// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.ExtentSource;

import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author jfoley
 */
final public class StreamExtentSource extends BTreeValueSource implements ExtentSource {
  public int documentCount;
  public int totalPositionCount;
  public int maximumPositionCount;  
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
    public long documentsByteFloor;
    public long countsByteFloor;
    public long positionsByteFloor;
  }
  // to support skipping
  private SkipState skip;
  
  // Supports lazy-loading of extents
  private boolean extentsLoaded;
  private int inlineMinimum;
  private int extentsByteSize;
  
  
  public StreamExtentSource(BTreeReader.BTreeIterator iter) throws IOException {
    super(iter);
    startPosition = btreeIter.getValueStart();
    endPosition = btreeIter.getValueEnd();
    extentArray = new ExtentArray();
    reset();
  }
  
  @Override
  public void reset() throws IOException {
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
    final DataStream valueStream = btreeIter.getSubValueStream(0, btreeIter.getValueLength());
    final DataInput stream = new VByteInput(valueStream);
    
    // metadata contained in options bitmap:
    final int options = stream.readInt();
    final boolean hasInlining = (options & HAS_INLINING) > 0;
    final boolean hasSkips = (options & HAS_SKIPS) > 0;
    final boolean hasMaxTF = (options & HAS_MAXTF) > 0;

    inlineMinimum = (hasInlining) ? stream.readInt() : Integer.MAX_VALUE;
    documentCount = stream.readInt();
    totalPositionCount = stream.readInt();
    maximumPositionCount = (hasMaxTF) ? stream.readInt() : Integer.MAX_VALUE;
   
    if (hasSkips) {
      skip = new SkipState();
      skip.distance = stream.readInt();
      skip.resetDistance = stream.readInt();
      skip.total = stream.readLong();
    }
    // segment lengths
    final long documentByteLength = stream.readLong();
    final long countsByteLength = stream.readLong();
    final long positionsByteLength = stream.readLong();
    final long skipsByteLength = hasSkips ? stream.readLong() : 0;
    final long skipPositionsByteLength = hasSkips ? stream.readLong() : 0;

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
    if (hasSkips) {
      long skipsStart = positionsStart + positionsByteLength;
      long skipPositionsStart = skipsStart + skipsByteLength;
      long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;
      assert skipPositionsEnd == endPosition - startPosition;
      skip.data = new VByteInput(btreeIter.getSubValueStream(skipsStart, skipsByteLength));
      skip.positionsStream = btreeIter.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
      skip.positions = new VByteInput(skip.positionsStream);
      // load up
      skip.nextDocument = skip.data.readInt();
      skip.documentsByteFloor = 0;
      skip.countsByteFloor = 0;
      skip.positionsByteFloor = 0;
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
      skip.documentsByteFloor = skip.positions.readInt();
      skip.countsByteFloor = skip.positions.readInt();
      skip.positionsByteFloor = skip.positions.readLong();
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
      documentsStream.seek(skip.documentsByteFloor);
      countsStream.seek(skip.countsByteFloor);
      positionsStream.seek(skip.positionsByteFloor);
    } else {
      skip.positionsStream.seek(skip.nextPosition);
      documentsStream.seek(skip.documentsByteFloor + skip.positions.readInt());
      countsStream.seek(skip.countsByteFloor + skip.positions.readInt());
      positionsStream.seek(skip.positionsByteFloor + skip.positions.readLong());
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

