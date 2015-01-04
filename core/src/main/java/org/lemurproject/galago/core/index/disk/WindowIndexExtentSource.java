// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.utility.btree.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.ExtentSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 *
 * @author jfoley
 */
public final class WindowIndexExtentSource extends BTreeValueSource implements ExtentSource {

  public long documentCount;
  public long totalWindowCount;
  public long maximumPositionCount;
  private VByteInput documents;
  private VByteInput counts;
  private VByteInput begins;
  private VByteInput ends;
  private long documentIndex;
  private long currentDocument;
  private int currentCount;
  private boolean done;
  private ExtentArray extentArray;
  // to support resets
  private long startPosition, endPosition;
  // to support skipping
  private VByteInput skips;
  private VByteInput skipPositions;
  private DataStream skipPositionsStream;
  private DataStream documentsStream;
  private DataStream countsStream;
  private DataStream beginsStream;
  private DataStream endsStream;
  private long skipDistance;
  private long skipResetDistance;
  private long numSkips;
  private long skipsRead;
  private long nextSkipDocument;
  private long lastSkipPosition;
  private long documentsByteFloor;
  private long countsByteFloor;
  private long beginsByteFloor;
  private long endsByteFloor;

  public WindowIndexExtentSource(BTreeIterator iter) throws IOException {
    super(iter);
    extentArray = new ExtentArray();
    reset();
  }

  @Override
  public void reset() throws IOException {
    startPosition = btreeIter.getValueStart();
    endPosition = btreeIter.getValueEnd();
    currentDocument = 0;
    currentCount = 0;
    done = false;
    extentArray.reset();
    initialize();
  }

  // Initialization method.
  //
  // Even though we check for skips multiple times, in terms of how the data is loaded
  // its easier to do the parts when appropriate
  protected void initialize() throws IOException {

    // need to read at most (15 + 90) bytes //
    DataStream valueStream = btreeIter.getSubValueStream(0, 150);
    DataInput stream = new VByteInput(valueStream);

    // metadata
    int options = stream.readInt(); // 5 bytes

    documentCount = stream.readLong(); // 9 bytes
    totalWindowCount = stream.readLong(); // 9 bytes
    maximumPositionCount = stream.readLong(); // 9 bytes

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipDistance = stream.readLong(); // 9 bytes
      skipResetDistance = stream.readLong(); // 9 bytes
      numSkips = stream.readLong(); // 9 bytes
    }

    // segment lengths
    long documentByteLength = stream.readLong(); // 9 bytes
    long countsByteLength = stream.readLong();// 9 bytes
    long beginsByteLength = stream.readLong();// 9 bytes
    long endsByteLength = stream.readLong();// 9 bytes
    long skipsByteLength = 0;
    long skipPositionsByteLength = 0;

    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      skipsByteLength = stream.readLong();// 9 bytes
      skipPositionsByteLength = stream.readLong();// 9 bytes
    }

    long documentStart = valueStream.getPosition();
    long countsStart = documentStart + documentByteLength;
    long beginsStart = countsStart + countsByteLength;
    long endsStart = beginsStart + beginsByteLength;
    long endsEnd = endsStart + endsByteLength;

    documentsStream = btreeIter.getSubValueStream(documentStart, documentByteLength);
    countsStream = btreeIter.getSubValueStream(countsStart, countsByteLength);
    beginsStream = btreeIter.getSubValueStream(beginsStart, beginsByteLength);
    endsStream = btreeIter.getSubValueStream(endsStart, endsByteLength);


    documents = new VByteInput(documentsStream);
    counts = new VByteInput(countsStream);
    begins = new VByteInput(beginsStream);
    ends = new VByteInput(endsStream);

    if ((options & HAS_SKIPS) == HAS_SKIPS) {

      long skipsStart = endsEnd;
      long skipPositionsStart = skipsStart + skipsByteLength;
      long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

      assert skipPositionsEnd == endPosition - startPosition;

      skips = new VByteInput(btreeIter.getSubValueStream(skipsStart, skipsByteLength));
      skipPositionsStream = btreeIter.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
      skipPositions = new VByteInput(skipPositionsStream);

      // load up
      nextSkipDocument = skips.readLong();
      documentsByteFloor = 0;
      countsByteFloor = 0;
      beginsByteFloor = 0;
      endsByteFloor = 0;
    } else {
      assert endsEnd == endPosition - startPosition;
      skips = null;
      skipPositions = null;
    }

    documentIndex = 0;
    loadExtents();
  }

  // Loads up a single set of positions for an intID. Basically it's the
  // load that needs to be done when moving forward one in the posting list.
  private void loadExtents() throws IOException {
    if (documentIndex >= documentCount) {
      done = true;
      currentDocument = Long.MAX_VALUE;
      extentArray.reset();
      currentCount = 0;
      return;
    }

    currentDocument += documents.readLong();
    currentCount = counts.readInt();
    extentArray.reset();

    extentArray.setDocument(currentDocument);
    int begin = 0;
    for (int i = 0; i < currentCount; i++) {
      begin += begins.readInt();
      int end = begin + ends.readInt();
      extentArray.add(begin, end);
    }
  }

  @Override
  public boolean isDone() {
    return done;
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
  public long currentCandidate() {
    return currentDocument;
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public void syncTo(long document) throws IOException {
    if (done) {
      return;
    }

    if (skips != null) {
      synchronizeSkipPositions();
    }
    if (skips != null && document > nextSkipDocument) {

      // if we're here, we're skipping
      while (skipsRead < numSkips && document > nextSkipDocument) {
        skipOnce();
      }
      repositionMainStreams();
    }

    // Linear from here
    while (!done && document > currentDocument) {
      documentIndex += 1;
      loadExtents();
    }
  }

  // This only moves forward in tier 1, reads from tier 2 only when
  // needed to update floors
  //
  private void skipOnce() throws IOException {
    assert skipsRead < numSkips;
    long currentSkipPosition = lastSkipPosition + skips.readLong();

    if (skipsRead % skipResetDistance == 0) {
      // Position the skip positions stream
      skipPositionsStream.seek(currentSkipPosition);

      // now set the floor values
      documentsByteFloor = skipPositions.readLong();
      countsByteFloor = skipPositions.readLong();
      beginsByteFloor = skipPositions.readLong();
      endsByteFloor = skipPositions.readLong();
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

  // This makes sure the skip list pointers are still ahead of the current document.
  // If we called "next" a lot, these may be out of sync.
  //
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
      beginsStream.seek(beginsByteFloor);
      endsStream.seek(endsByteFloor);
    } else {
      skipPositionsStream.seek(lastSkipPosition);
      documentsStream.seek(documentsByteFloor + skipPositions.readLong());
      countsStream.seek(countsByteFloor + skipPositions.readLong());
      beginsStream.seek(beginsByteFloor + skipPositions.readLong());
      endsStream.seek(endsByteFloor + skipPositions.readLong());
    }
    documentIndex = (int) (skipDistance * skipsRead) - 1;
  }

  @Override
  public ExtentArray extents(long id) {
    if (!done && id == currentCandidate()) {
      return extentArray;
    }
    return ExtentArray.EMPTY;
  }

  @Override
  public NodeStatistics getStatistics() {
    NodeStatistics stats = new NodeStatistics();
    stats.node = this.key();
    stats.nodeFrequency = this.totalWindowCount;
    stats.nodeDocumentCount = this.documentCount;
    stats.maximumCount = this.maximumPositionCount;
    return stats;
  }

  @Override
  public int count(long id) {
    if (!done && id == currentCandidate()) {
      return currentCount;
    }
    return 0;
  }
}
