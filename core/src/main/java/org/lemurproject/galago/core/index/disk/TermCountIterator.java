// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * This iterator simply ignores the positions information - faster b/c when
 * incrementing or loading or skipping, we don't have to bookkeep the
 * positions buffer. Overall smaller footprint and faster execution.
 *
 */
public class TermCountIterator extends BTreeValueIterator implements NodeAggregateIterator, CountIterator {
  BTreeReader.BTreeIterator iterator;
  int documentCount;
  int collectionCount;
  int maximumPositionCount;
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

  public TermCountIterator(BTreeReader.BTreeIterator iterator) throws IOException {
    super(iterator.getKey());
    reset(iterator);
  }

  // Initialization method.
  //
  // Even though we check for skips multiple times, in terms of how the data is loaded
  // its easier to do the parts when appropriate
  protected void initialize() throws IOException {
    DataStream valueStream = iterator.getSubValueStream(0, iterator.getValueLength());
    DataInput stream = new VByteInput(valueStream);
    // metadata
    int options = stream.readInt();
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
    documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
    countsStream = iterator.getSubValueStream(countsStart, countsByteLength);
    documents = new VByteInput(documentsStream);
    counts = new VByteInput(countsStream);
    if ((options & HAS_SKIPS) == HAS_SKIPS) {
      long skipsStart = positionsStart + positionsByteLength;
      long skipPositionsStart = skipsStart + skipsByteLength;
      long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;
      assert skipPositionsEnd == endPosition - startPosition;
      skips = new VByteInput(iterator.getSubValueStream(skipsStart, skipsByteLength));
      skipPositionsStream = iterator.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
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

  // Only loading the docid and the count
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
  public String getValueString() throws IOException {
    StringBuilder builder = new StringBuilder();
    builder.append(getKeyString());
    builder.append(",");
    builder.append(currentDocument);
    builder.append(",");
    builder.append(currentCount);
    return builder.toString();
  }

  @Override
  public void reset(BTreeReader.BTreeIterator i) throws IOException {
    iterator = i;
    startPosition = iterator.getValueStart();
    endPosition = iterator.getValueEnd();
    initialize();
  }

  @Override
  public void reset() throws IOException {
    currentDocument = 0;
    currentCount = 0;
    done = false;
    initialize();
  }

  @Override
  public void movePast(int document) throws IOException {
    syncTo(document + 1);
  }

  // If we have skips - it's go time
  @Override
  public void syncTo(int document) throws IOException {
    if (done) {
      return;
    }
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

  // This only moves forward in tier 1, reads from tier 2 only when
  // needed to update floors
  //
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

  // This makes sure the skip list pointers are still ahead of the current document.
  // If we called "next" a lot, these may be out of sync.
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
  public boolean isDone() {
    return done;
  }

  @Override
  public int currentCandidate() {
    return currentDocument;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public int count() {
    if (!done && currentCandidate() == context.document) {
      return currentCount;
    }
    return 0;
  }

  @Override
  public int maximumCount() {
    return maximumPositionCount;
  }

  @Override
  public long totalEntries() {
    return documentCount;
  }

  public NodeStatistics getStatistics() {
    NodeStatistics stats = new NodeStatistics();
    stats.node = Utility.toString(this.key);
    stats.nodeFrequency = this.collectionCount;
    stats.nodeDocumentCount = this.documentCount;
    stats.maximumCount = this.maximumPositionCount;
    return stats;
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "counts";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = Integer.toString(count());
    List<AnnotatedNode> children = Collections.EMPTY_LIST;
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
  
}
