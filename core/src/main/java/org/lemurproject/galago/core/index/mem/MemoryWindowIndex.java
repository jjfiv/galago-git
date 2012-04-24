// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.AggregateReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIterator;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.disk.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.index.mem.MemoryWindowIndex.ExtentList.ExtentIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.ContextualIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.ModifiableIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Utility.ByteArrComparator;
import org.lemurproject.galago.tupleflow.VByteInput;

/*
 * author sjh
 *
 * In-memory posting index
 */
public class MemoryWindowIndex implements MemoryIndexPart, AggregateReader {

  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  private TreeMap<byte[], ExtentList> extents = new TreeMap(new ByteArrComparator());
  private Parameters parameters;
  private long collectionDocumentCount = 0;
  private long collectionPostingCount = 0;

  public MemoryWindowIndex(Parameters parameters) {
    this.parameters = parameters;
    this.parameters.set("writerClass", "org.lemurproject.galago.core.index.ExtentIndexWriter");
  }

  @Override
  public void addDocument(Document doc) {
    collectionDocumentCount += 1;
    collectionPostingCount += doc.terms.size();

    int prevBegin = -1;
    for (Tag tag : doc.tags) {
      assert tag.begin >= prevBegin;
      prevBegin = tag.begin;
      addExtent(Utility.fromString(tag.name), doc.identifier, tag.begin, tag.end);
    }
  }

  @Override
  public void addIteratorData(byte[] key, MovableIterator iterator) throws IOException {

    if (extents.containsKey(key)) {
      // do nothing - we have already cached this data
      return;
    }

    ExtentList extentList = new ExtentList(key);
    
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      ExtentArrayIterator extentsIterator = new ExtentArrayIterator(((MovableExtentIterator) iterator).extents());
      while (!extentsIterator.isDone()) {
        int begin = extentsIterator.currentBegin();
        int end = extentsIterator.currentEnd();

        extentList.add(document, begin, end);
        extentsIterator.next();
      }
      iterator.next();
    }
    extents.put(key, extentList);
  }

  private void addExtent(byte[] byteExtentName, int document, int begin, int end) {
    ExtentList extentList;
    if (!extents.containsKey(byteExtentName)) {
      extentList = new ExtentList(byteExtentName);
      extents.put(byteExtentName, extentList);
    }

    extentList = extents.get(byteExtentName);
    extentList.add(document, begin, end);
  }

  // Posting List Reader functions
  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    KeyIterator i = getIterator();
    i.skipToKey(Utility.fromString(node.getDefaultParameter()));
    if (0 == Utility.compare(i.getKey(), Utility.fromString(node.getDefaultParameter()))) {
      return i.getValueIterator();
    }
    return null;
  }

  // try to free up memory.
  @Override
  public void close() throws IOException {
    extents = null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(ExtentIterator.class));
    types.put("extents", new NodeType(ExtentIterator.class));
    return types;
  }

  @Override
  public NodeStatistics getTermStatistics(String term) throws IOException {
    return getTermStatistics(Utility.fromString(term));
  }

  @Override
  public NodeStatistics getTermStatistics(byte[] term) throws IOException {
    return extents.get(term).getExtentIterator().getStatistics();
  }

  @Override
  public String getDefaultOperator() {
    return "extents";
  }

  @Override
  public Parameters getManifest() {
    return parameters;
  }

  @Override
  public long getDocumentCount() {
    return collectionDocumentCount;
  }

  @Override
  public long getCollectionLength() {
    return collectionPostingCount;
  }

  @Override
  public long getKeyCount() {
    return this.extents.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(p));

    KIterator kiterator = new KIterator();
    MovableExtentIterator viterator;
    ExtentArray exts;
    while (!kiterator.isDone()) {
      viterator = (MovableExtentIterator) kiterator.getValueIterator();
      writer.processExtentName(kiterator.getKey());

      while (!viterator.isDone()) {
        writer.processNumber(viterator.currentCandidate());
        exts = viterator.extents();
        for (int i = 0; i < exts.size(); i++) {
          writer.processBegin(exts.begin(i));
          writer.processTuple(exts.end(i));
        }
        viterator.next();
      }
      kiterator.nextKey();
    }
    writer.close();
  }

  // iterator allows for query processing and for streaming posting list data
  //  public class Iterator extends ExtentIterator implements IndexIterator {
  public class KIterator implements KeyIterator {

    Iterator<byte[]> iterator;
    byte[] currKey;
    boolean done = false;

    public KIterator() throws IOException {
      iterator = extents.keySet().iterator();
      this.nextKey();
    }

    @Override
    public void reset() throws IOException {
      iterator = extents.keySet().iterator();
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(currKey);
    }

    @Override
    public byte[] getKey() {
      return currKey;
    }

    @Override
    public boolean nextKey() throws IOException {
      if (iterator.hasNext()) {
        currKey = iterator.next();
        return true;
      } else {
        currKey = null;
        done = true;
        return false;
      }
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      iterator = extents.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      iterator = extents.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public String getValueString() throws IOException {
      long count = -1;
      ExtentIterator it = extents.get(currKey).getExtentIterator();
      count = it.count();
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
    public byte[] getValueBytes() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return extents.get(currKey).getExtentIterator();
    }
  }

  // sub classes:
  public class ExtentList {

    private final byte[] m_termBytes;
    CompressedByteBuffer documents_cbb = new CompressedByteBuffer();
    CompressedByteBuffer counts_cbb = new CompressedByteBuffer();
    CompressedByteBuffer begins_cbb = new CompressedByteBuffer();
    CompressedByteBuffer ends_cbb = new CompressedByteBuffer();
    //IntArray documents = new IntArray();
    //IntArray termFreqCounts = new IntArray();
    //IntArray termPositions = new IntArray();
    int extentDocumentCount = 0;
    int extentPostingCount = 0;
    int lastDocument = 0;
    int lastCount = 0;
    int lastBegin = 0;
    int lastEnd = 0;

    public ExtentList(byte[] termBytes) {
      m_termBytes = termBytes;
    }

    public void add(int document, int begin, int end) {
      if (extentDocumentCount == 0) {
        // first instance of term
        lastDocument = document;
        lastCount = 1;
        extentDocumentCount += 1;
        documents_cbb.add(document);
      } else if (lastDocument == document) {
        // additional instance of term in document
        lastCount += 1;
      } else {
        // new document
        assert lastDocument == 0 || document > lastDocument;
        documents_cbb.add(document - lastDocument);
        lastDocument = document;
        counts_cbb.add(lastCount);
        lastCount = 1;
        extentDocumentCount += 1;
        lastBegin = 0;
        lastEnd = 0;
      }
      assert begin >= lastBegin;
      begins_cbb.add(begin - lastBegin);
      ends_cbb.add(end);
      extentPostingCount += 1;
      lastBegin = 0;
    }

    private ExtentIterator getExtentIterator() throws IOException {
      return new ExtentIterator();
    }

    public class ExtentIterator extends ValueIterator implements AggregateIterator, MovableCountIterator, MovableExtentIterator {

      VByteInput documents_reader;
      VByteInput counts_reader;
      VByteInput begins_reader;
      VByteInput ends_reader;
      int iteratedDocs;
      int currDocument;
      int currCount;
      ExtentArray extents;
      boolean done;

      public ExtentIterator() throws IOException {
        reset();
      }

      @Override
      public void reset() throws IOException {
        documents_reader = new VByteInput(
                new DataInputStream(
                new ByteArrayInputStream(documents_cbb.getBytes())));
        counts_reader = new VByteInput(
                new DataInputStream(
                new ByteArrayInputStream(counts_cbb.getBytes())));
        begins_reader = new VByteInput(
                new DataInputStream(
                new ByteArrayInputStream(begins_cbb.getBytes())));
        ends_reader = new VByteInput(
                new DataInputStream(
                new ByteArrayInputStream(ends_cbb.getBytes())));

        iteratedDocs = 0;
        currDocument = 0;
        currCount = 0;
        extents = new ExtentArray();

        next();
      }

      @Override
      public int count() {
        return currCount;
      }

      @Override
      public int maximumCount() {
        return Integer.MAX_VALUE;
      }

      @Override
      public ExtentArray extents() {
        return extents;
      }

      @Override
      public ExtentArray getData() {
        return extents;
      }

      @Override
      public boolean isDone() {
        return done;
      }

      @Override
      public int currentCandidate() {
        return currDocument;
      }

      @Override
      public boolean atCandidate(int identifier) {
        return (!isDone() && identifier == currDocument);
      }

      @Override
      public boolean hasAllCandidates() {
        return false;
      }

      @Override
      public void next() throws IOException {
        if (iteratedDocs >= extentDocumentCount) {
          done = true;
          return;
        } else if (iteratedDocs == extentDocumentCount - 1) {
          currDocument = lastDocument;
          currCount = lastCount;
        } else {
          currDocument += documents_reader.readInt();
          currCount = counts_reader.readInt();
        }
        loadExtents();

        iteratedDocs++;
      }

      public void loadExtents() throws IOException {
        extents.reset();
        extents.setDocument(currDocument);
        int begin = 0;
        int end;
        for (int i = 0; i < currCount; i++) {
          begin += begins_reader.readInt();
          end = ends_reader.readInt();
          extents.add(begin, end);
        }
      }

      @Override
      public void moveTo(int identifier) throws IOException {
        while (!isDone() && (currDocument < identifier)) {
          next();
        }
      }

      @Override
      public void movePast(int identifier) throws IOException {
        moveTo(identifier + 1);
      }

      @Override
      public String getEntry() throws IOException {
        StringBuilder builder = new StringBuilder();

        builder.append(Utility.toString(m_termBytes));
        builder.append(",");
        builder.append(currDocument);
        for (int i = 0; i < extents.size(); ++i) {
          builder.append(",");
          builder.append(extents.begin(i));
        }

        return builder.toString();
      }

      @Override
      public long totalEntries() {
        return extentDocumentCount;
      }

      @Override
      public NodeStatistics getStatistics() {
        NodeStatistics stats = new NodeStatistics();
        stats.node = Utility.toString(m_termBytes);
        stats.nodeFrequency = extentPostingCount;
        stats.nodeDocumentCount = extentDocumentCount;
        stats.collectionLength = collectionPostingCount;
        stats.documentCount = collectionDocumentCount;
        return stats;
      }

      @Override
      public int compareTo(MovableIterator other) {
        if (isDone() && !other.isDone()) {
          return 1;
        }
        if (other.isDone() && !isDone()) {
          return -1;
        }
        if (isDone() && other.isDone()) {
          return 0;
        }
        return currentCandidate() - other.currentCandidate();
      }
      
      @Override
      public String getKeyString() throws IOException {
        return Utility.toString(m_termBytes);
      }

      @Override
      public byte[] getKeyBytes() throws IOException {
        return m_termBytes;
      }
    }
  }
}
