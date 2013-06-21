// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Reads a simple positions-based index, where each inverted list in the index
 * contains both term count information and term position information. The term
 * counts data is stored separately from term position information for faster
 * query processing when no positions are needed.
 *
 * @author sjh, irmarc, 
 */
public class WindowIndexReader extends KeyListReader implements AggregateIndexPart {

  Stemmer stemmer = null;

  public WindowIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  public WindowIndexReader(String pathname) throws Exception {
    super(pathname);
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public WindowExtentIterator getTermExtents(String term) throws IOException {
    term = stemAsRequired(term);
    BTreeReader.BTreeIterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator != null) {
      return new WindowExtentIterator(iterator);
    }
    return null;
  }

  public WindowCountIterator getTermCounts(String term) throws IOException {
    term = stemAsRequired(term);
    BTreeReader.BTreeIterator iterator = reader.getIterator(Utility.fromString(term));

    if (iterator != null) {
      return new WindowCountIterator(iterator, this);
    }
    return null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(WindowCountIterator.class));
    types.put("extents", new NodeType(WindowExtentIterator.class));
    return types;
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      return getTermCounts(node.getDefaultParameter());
    } else {
      return getTermExtents(node.getDefaultParameter());
    }
  }

  @Override
  public IndexPartStatistics getStatistics() {
    Parameters manifest = this.getManifest();
    IndexPartStatistics is = new IndexPartStatistics();
    is.collectionLength = manifest.get("statistics/collectionLength", 0);
    is.vocabCount = manifest.get("statistics/vocabCount", 0);
    is.highestDocumentCount = manifest.get("statistics/highestDocumentCount", 0);
    is.highestFrequency = manifest.get("statistics/highestFrequency", 0);
    is.partName = manifest.get("filename", "CountIndexPart");
    return is;
  }

  private String stemAsRequired(String window) {
    if (stemmer != null) {
      if (window.contains("~")) {
        return stemmer.stemWindow(window);
      } else {
        return stemmer.stem(window);
      }
    }
    return window;
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      WindowCountIterator it;
      long count = -1;
      try {
        it = new WindowCountIterator(iterator, org.lemurproject.galago.core.index.disk.WindowIndexReader.this);
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

    public DiskIterator getValueIterator() throws IOException {
      return new WindowExtentIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }
  }

  public class WindowExtentIterator extends BTreeValueIterator
          implements NodeAggregateIterator, CountIterator, ExtentIterator {

    private BTreeReader.BTreeIterator iterator;
    int documentCount;
    int totalWindowCount;
    int maximumPositionCount;
    private VByteInput documents;
    private VByteInput counts;
    private VByteInput begins;
    private VByteInput ends;
    private int documentIndex;
    int currentDocument;
    private int currentCount;
    private boolean done;
    private ExtentArray extentArray;
    final ExtentArray emptyExtentArray;
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
    private int skipDistance;
    private int skipResetDistance;
    private long numSkips;
    private long skipsRead;
    private long nextSkipDocument;
    private long lastSkipPosition;
    private long documentsByteFloor;
    private long countsByteFloor;
    private long beginsByteFloor;
    private long endsByteFloor;

    public WindowExtentIterator(BTreeReader.BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      extentArray = new ExtentArray();
      emptyExtentArray = new ExtentArray();
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
      totalWindowCount = stream.readInt();

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
      long beginsByteLength = stream.readLong();
      long endsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = valueStream.getPosition();
      long countsStart = documentStart + documentByteLength;
      long beginsStart = countsStart + countsByteLength;
      long endsStart = beginsStart + beginsByteLength;
      long endsEnd = endsStart + endsByteLength;

      documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
      countsStream = iterator.getSubValueStream(countsStart, countsByteLength);
      beginsStream = iterator.getSubValueStream(beginsStart, beginsByteLength);
      endsStream = iterator.getSubValueStream(endsStart, endsByteLength);


      documents = new VByteInput(documentsStream);
      counts = new VByteInput(countsStream);
      begins = new VByteInput(beginsStream);
      ends = new VByteInput(endsStream);

      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = endsStart + endsByteLength;
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
        currentDocument = Integer.MAX_VALUE;
        extentArray.reset();
        currentCount = 0;
        return;
      }

      currentDocument += documents.readInt();
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
    public String getValueString() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(getKeyString());
      builder.append(",");
      builder.append(currentDocument);
      for (int i = 0; i < extentArray.size(); ++i) {
        builder.append(",");
        builder.append(extentArray.begin(i));
        builder.append("-");
        builder.append(extentArray.end(i));
      }

      return builder.toString();
    }

    @Override
    public void reset(BTreeReader.BTreeIterator i) throws IOException {
      iterator = i;
      key = iterator.getKey();
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      reset();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      done = false;
      extentArray.reset();
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
      }
      if (skips != null && document > nextSkipDocument) {

        // if we're here, we're skipping
        while (skipsRead < numSkips
                && document > nextSkipDocument) {
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
      long currentSkipPosition = lastSkipPosition + skips.readInt();

      if (skipsRead % skipResetDistance == 0) {
        // Position the skip positions stream
        skipPositionsStream.seek(currentSkipPosition);

        // now set the floor values
        documentsByteFloor = skipPositions.readInt();
        countsByteFloor = skipPositions.readInt();
        beginsByteFloor = skipPositions.readLong();
        endsByteFloor = skipPositions.readLong();
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
      while (!done && nextSkipDocument <= currentDocument) {
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
        beginsStream.seek(beginsByteFloor);
        endsStream.seek(endsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        beginsStream.seek(beginsByteFloor + skipPositions.readLong());
        endsStream.seek(endsByteFloor + skipPositions.readLong());
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public ExtentArray getData() {
      return extentArray;
    }

    @Override
    public ExtentArray extents() {
      if (!done && context.document == this.currentDocument) {
        return extentArray;
      }
      return this.emptyExtentArray;
    }

    @Override
    public int count() {
      if (!done && context.document == this.currentDocument) {
        return currentCount;
      }
      return 0;
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
    public long totalEntries() {
      return ((long) documentCount);
    }

    @Override
    public NodeStatistics getStatistics() {
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(this.key);
      stats.nodeFrequency = this.totalWindowCount;
      stats.nodeDocumentCount = this.documentCount;
      stats.maximumCount = this.maximumPositionCount;
      return stats;
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "extents";
      String className = this.getClass().getSimpleName();
      String parameters = this.getKeyString();
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = extents().toString();
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }

  /**
   * This iterator simply ignores the positions information - faster b/c when
   * incrementing or loading or skipping, we don't have to bookkeep the
   * positions buffer. Overall smaller footprint and faster execution.
   *
   */
  public class WindowCountIterator extends BTreeValueIterator
          implements NodeAggregateIterator, CountIterator {

    BTreeReader.BTreeIterator iterator;
    int documentCount;
    int collectionCount;
    VByteInput documents;
    VByteInput counts;
    int documentIndex;
    int currentDocument;
    int currentCount;
    boolean done;
    int maximumPositionCount;
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

    public WindowCountIterator(BTreeReader.BTreeIterator iterator, KeyListReader outer) throws IOException {
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
      long beginsByteLength = stream.readLong();
      long endsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = valueStream.getPosition();
      long countsStart = documentStart + documentByteLength;
      long beginsStart = countsStart + countsByteLength;
      long endsStart = beginsStart + beginsByteLength;
      long endsEnd = endsStart + endsByteLength;

      documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
      countsStream = iterator.getSubValueStream(countsStart, countsByteLength);


      documents = new VByteInput(documentsStream);
      counts = new VByteInput(countsStream);

      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = endsStart + endsByteLength;
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
        assert endsEnd == endPosition - startPosition;
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
      key = iterator.getKey();
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
      while (!done && nextSkipDocument <= currentDocument) {
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
      if (!done && context.document == this.currentDocument) {
        return currentCount;
      }
      return 0;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
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
}
