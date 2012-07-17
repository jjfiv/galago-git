// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Reads a count based index structure mapping( term -> list(document-id),
 * list(document-freq) )
 *
 * Skip lists are supported
 *
 * @author sjh
 */
public class CountIndexReader extends KeyListReader implements AggregateReader {

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      TermCountIterator it;
      long count = -1;
      try {
        it = new TermCountIterator(iterator);
        count = it.count();
      } catch (IOException ioe) {
      }
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKey())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new TermCountIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }
  }

  public class TermCountIterator extends KeyListReader.ListIterator
          implements AggregateIterator, MovableCountIterator {

    BTreeReader.BTreeIterator iterator;
    int documentCount;
    int collectionCount;
    int maximumPositionCount;
    VByteInput documents;
    VByteInput counts;
    int documentIndex;
    int currentDocument;
    int currentCount;
    // Support for resets
    long startPosition, endPosition;
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

      documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
      countsStream = iterator.getSubValueStream(countsStart, countsByteLength);

      documents = new VByteInput(documentsStream);
      counts = new VByteInput(countsStream);

      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = countsEnd;
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
        assert countsEnd == endPosition - startPosition;
        skips = null;
        skipPositions = null;
      }

      documentIndex = 0;
      load();
    }

    // Only loading the docid and the count
    private void load() throws IOException {
      currentDocument += documents.readInt();
      currentCount = counts.readInt();
    }

    @Override
    public String getEntry() throws IOException {
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
      key = iterator.getKey();
      initialize();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      initialize();
    }

    // If we have skips - it's go time
    @Override
    public void moveTo(int document) throws IOException {
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
    //
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
      return documentIndex >= documentCount;
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
      return currentCount;
    }

    @Override
    public int maximumCount() {
      return maximumPositionCount;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      if (modifiers != null && modifiers.containsKey("background")) {
        return (NodeStatistics) modifiers.get("background");
      }
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(this.key);
      stats.nodeFrequency = this.collectionCount;
      stats.nodeDocumentCount = this.documentCount;
      stats.collectionLength = reader.getManifest().get("statistics/collectionLength", -1);
      stats.documentCount = reader.getManifest().get("statistics/documentCount", -1);
      return stats;
    }

    @Override
    public AnnotatedNode getAnnotatedNode() {
      String type = "count";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Integer.toString(count());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }

  public CountIndexReader(BTreeReader reader) throws IOException {
    super(reader);
  }

  public CountIndexReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public TermCountIterator getTermCounts(byte[] key) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(key);

    if (iterator != null) {
      return new TermCountIterator(iterator);
    }
    return null;
  }

  public TermCountIterator getTermCounts(String term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(Utility.fromString(term));

    if (iterator != null) {
      return new TermCountIterator(iterator);
    }
    return null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(TermCountIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      return getTermCounts(node.getDefaultParameter());
    }
    return null;
  }

  @Override
  public NodeStatistics getTermStatistics(String term) throws IOException {
    return getTermStatistics(Utility.fromString(term));
  }

  @Override
  public NodeStatistics getTermStatistics(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);

    if (iterator != null) {
      TermCountIterator termCountIterator = new TermCountIterator(iterator);
      return termCountIterator.getStatistics();
    }
    NodeStatistics stats = new NodeStatistics();
    stats.node = Utility.toString(term);
    return stats;
  }
}
