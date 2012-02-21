// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader;
import org.lemurproject.galago.core.index.GenericIndexReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ExtentValueIterator;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Reads a simple positions-based index, where each inverted list in the
 * index contains both term count information and term position information.
 * The term counts data is stored separately from term position information for
 * faster query processing when no positions are needed.
 *
 * (12/16/2010, irmarc): In order to facilitate faster count-only processing,
 *                        the default iterator created will not even open the
 *                        positions list when iterating. This is an interesting
 *                        enough change that there are now two versions of the iterator
 *
 * @author trevor, irmarc
 */
public class WindowIndexReader extends KeyListReader implements AggregateReader {

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
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
      sb.append(Utility.toString(getKeyBytes())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    public ValueIterator getValueIterator() throws IOException {
      return new TermExtentIterator(iterator);
    }
  }

  public class TermExtentIterator extends KeyListReader.ListIterator
          implements AggregateIterator, CountValueIterator, ExtentValueIterator {

    GenericIndexReader.Iterator iterator;
    int documentCount;
    int totalWindowCount;
    int maximumPositionCount;
    VByteInput documents;
    VByteInput counts;
    VByteInput begins;
    VByteInput ends;
    int documentIndex;
    int currentDocument;
    int currentCount;
    ExtentArray extentArray;
    // to support resets
    long startPosition, endPosition;
    // to support skipping
    VByteInput skips;
    VByteInput skipPositions;
    DataStream skipPositionsStream;
    DataStream documentsStream;
    DataStream countsStream;
    DataStream beginsStream;
    DataStream endsStream;
    int skipDistance;
    int skipResetDistance;
    long numSkips;
    long skipsRead;
    long nextSkipDocument;
    long lastSkipPosition;
    long documentsByteFloor;
    long countsByteFloor;
    long windowsByteFloor;

    TermExtentIterator(GenericIndexReader.Iterator iterator) throws IOException {
      extentArray = new ExtentArray();
      reset(iterator);
    }

    // Initialization method.
    //
    // Even though we check for skips multiple times, in terms of how the data is loaded
    // its easier to do the parts when appropriate
    protected void initialize() throws IOException {
      DataStream valueStream = iterator.getSubValueStream(0, dataLength);
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
        windowsByteFloor = 0;
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
    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
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
    public void reset(GenericIndexReader.Iterator i) throws IOException {
      iterator = i;
      key = iterator.getKey();
      dataLength = iterator.getValueLength();
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      reset();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      extentArray.reset();
      initialize();
    }

    @Override
    public boolean next() throws IOException {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      if (!isDone()) {
        loadExtents();
        return true;
      }
      return false;
    }

    // If we have skips - it's go time
    @Override
    public boolean moveTo(int document) throws IOException {
      if (skips != null && document > nextSkipDocument) {

        // if we're here, we're skipping
        while (skipsRead < numSkips
                && document > nextSkipDocument) {
          skipOnce();
        }
        repositionMainStreams();
      }

      // Linear from here
      while (document > currentDocument && next());
      return hasMatch(document);
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
        windowsByteFloor = skipPositions.readLong();
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
        beginsStream.seek(windowsByteFloor);
        endsStream.seek(windowsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        beginsStream.seek(windowsByteFloor + skipPositions.readLong());
        endsStream.seek(windowsByteFloor + skipPositions.readLong());
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    @Override
    public boolean isDone() {
      return documentIndex >= documentCount;
    }

    @Override
    public ExtentArray getData() {
      return extentArray;
    }

    @Override
    public ExtentArray extents() {
      return extentArray;
    }

    @Override
    public int currentCandidate() {
      return currentDocument;
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
      return ((long) documentCount);
    }

    @Override
    public NodeStatistics getStatistics() {
      if (modifiers != null && modifiers.containsKey("background")) {
        return (NodeStatistics) modifiers.get("background");
      }
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(this.key);
      stats.nodeFrequency = this.totalWindowCount;
      stats.nodeDocumentCount = this.documentCount;
      stats.collectionLength = reader.getManifest().get("statistics/collectionLength", -1);
      stats.documentCount = reader.getManifest().get("statistics/documentCount", -1);
      return stats;
    }
  }

  /**
   * This iterator simply ignores the positions information - faster b/c when incrementing or loading or skipping,
   * we don't have to bookkeep the positions buffer. Overall smaller footprint and faster execution.
   *
   */
  public class TermCountIterator extends KeyListReader.ListIterator
          implements AggregateIterator, CountValueIterator {

    GenericIndexReader.Iterator iterator;
    int documentCount;
    int collectionCount;
    VByteInput documents;
    VByteInput counts;
    int documentIndex;
    int currentDocument;
    int currentCount;
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

    TermCountIterator(GenericIndexReader.Iterator iterator) throws IOException {
      reset(iterator);
    }

    // Initialization method.
    //
    // Even though we check for skips multiple times, in terms of how the data is loaded
    // its easier to do the parts when appropriate
    protected void initialize() throws IOException {
      DataStream valueStream = iterator.getSubValueStream(0, dataLength);
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
      currentDocument += documents.readInt();
      currentCount = counts.readInt();
    }

    @Override
    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentCount);

      return builder.toString();
    }

    @Override
    public void reset(GenericIndexReader.Iterator i) throws IOException {
      iterator = i;
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      dataLength = iterator.getValueLength();
      key = iterator.getKey();
      initialize();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      initialize();
    }

    @Override
    public boolean next() throws IOException {
      documentIndex = Math.min(documentIndex + 1, documentCount);
      if (!isDone()) {
        load();
        return true;
      }
      return false;
    }

    // If we have skips - it's go time
    @Override
    public boolean moveTo(int document) throws IOException {
      if (skips != null && document > nextSkipDocument) {
        // if we're here, we're skipping
        while (skipsRead < numSkips
                && document > nextSkipDocument) {
          skipOnce();
        }
        repositionMainStreams();
      }

      // linear from here
      while (document > currentDocument && next());
      return hasMatch(document);
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
        skipPositions.readLong(); // throw away, but we have to move it forward
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

    // This will pass up topdocs information if it's available
    public void setContext(ScoringContext context) {
      if (TopDocsContext.class.isAssignableFrom(context.getClass())
              && this.hasModifier("topdocs")) {
        ((TopDocsContext) context).hold = ((ArrayList<TopDocument>) getModifier("topdocs"));
        // remove the pointer to the mod (don't need it anymore)
        this.modifiers.remove("topdocs");
      }
    }
  }
  Stemmer stemmer = null;

  public WindowIndexReader(GenericIndexReader reader) throws Exception {
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
   * Returns an iterator pointing at the specified term, or
   * null if the term doesn't exist in the inverted file.
   */
  public TermExtentIterator getTermExtents(String term) throws IOException {
    term = stemAsRequired(term);
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator != null) {
      return new TermExtentIterator(iterator);
    }
    return null;
  }

  public TermCountIterator getTermCounts(String term) throws IOException {
    term = stemAsRequired(term);
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));

    if (iterator != null) {
      return new TermCountIterator(iterator);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(TermCountIterator.class));
    types.put("extents", new NodeType(TermExtentIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    String term = stemAsRequired(node.getDefaultParameter());
    if (node.getOperator().equals("counts")) {
      return getTermCounts(term);
    } else {
      return getTermExtents(term);
    }
  }

  @Override
  public NodeStatistics getTermStatistics(String term) throws IOException {
    term = stemAsRequired(term);
    return getTermStatistics(Utility.fromString(term));
  }

  @Override
  public NodeStatistics getTermStatistics(byte[] term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(term);

    if (iterator != null) {
      TermCountIterator termCountIterator = new TermCountIterator(iterator);
      return termCountIterator.getStatistics();
    }
    NodeStatistics stats = new NodeStatistics();
    stats.node = Utility.toString(term);
    return stats;
  }

  private String stemAsRequired(String window) {
    if (stemmer != null) {
      // window from: sample~sample~sample
      //          to: sampl~sampl~sampl
      String[] terms = window.split("~");
      StringBuilder reconstructor = new StringBuilder();
      boolean first = true;
      for (String term : terms) {
        if (!first) {
          reconstructor.append("~");
        }
        first = false;
        reconstructor.append(stemmer.stem(term));
      }
      return reconstructor.toString();
    }

    // otherwise no change.
    return window;
  }
}
