// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.PositionIndexWriter;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.SourceIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Utility.ByteArrComparator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


/*
 * author sjh
 *
 * In-memory posting index
 *
 */
public class MemoryPositionalIndex implements MemoryIndexPart, AggregateIndexPart {

  // this could be a bit big -- but we need random access here
  // perhaps we should use a trie (but java doesn't have one?)
  protected TreeMap<byte[], PositionalPostingList> postings = new TreeMap<byte[], PositionalPostingList>(new ByteArrComparator());
  protected Parameters parameters;
  protected long collectionDocumentCount = 0;
  protected long collectionPostingsCount = 0;
  protected long vocabCount = 0;
  protected long highestFrequency = 0;
  protected long highestDocumentCount = 0;
  protected Stemmer stemmer = null;

  public MemoryPositionalIndex(Parameters parameters) throws Exception {
    this.parameters = parameters;

    if (parameters.containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(parameters.getString("stemmer")).newInstance();
    }

  }

  @Override
  public void addDocument(Document doc) throws IOException {
    int position = 0;
    for (String term : doc.terms) {
      String stem = stemAsRequired(term);
      if (stem != null) {
        addPosting(ByteUtil.fromString(stem), doc.identifier, position);
        position += 1;
      }
    }

    collectionDocumentCount += 1;
    collectionPostingsCount += doc.terms.size();
    vocabCount = postings.size();
  }

  @Override
  public void addIteratorData(byte[] key, BaseIterator iterator) throws IOException {

    if (postings.containsKey(key)) {
      // do nothing - we have already cached this data
      return;
    }

    PositionalPostingList postingList = new PositionalPostingList(key);
    ExtentIterator mi = (ExtentIterator) iterator;
    ScoringContext sc = new ScoringContext();
    while (!mi.isDone()) {
      long document = mi.currentCandidate();
      sc.document = document;
      ExtentArrayIterator ei = new ExtentArrayIterator(mi.extents(sc));
      while (!ei.isDone()) {
        postingList.add(document, ei.currentBegin());
        ei.next();
      }
      mi.movePast(document);
    }

    postings.put(key, postingList);

    this.highestDocumentCount = Math.max(highestDocumentCount, postingList.termDocumentCount);
    this.highestFrequency = Math.max(highestFrequency, postingList.termPostingsCount);
    this.vocabCount = postings.size();
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    postings.remove(key);
  }

  protected void addPosting(byte[] byteWord, long document, int position) {
    if (!postings.containsKey(byteWord)) {
      PositionalPostingList postingList = new PositionalPostingList(byteWord);
      postings.put(byteWord, postingList);
    }

    PositionalPostingList postingList = postings.get(byteWord);
    postingList.add(document, position);

    // this posting list has changed - check if the aggregate stats also need to change.
    this.highestDocumentCount = Math.max(highestDocumentCount, postingList.termDocumentCount);
    this.highestFrequency = Math.max(highestFrequency, postingList.termPostingsCount);
  }

  // Posting List Reader functions
  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public SourceIterator getIterator(Node node) throws IOException {
    String term = stemAsRequired(node.getDefaultParameter());
    byte[] byteWord = ByteUtil.fromString(term);
    if (node.getOperator().equals("counts")) {
      return getTermCounts(byteWord);
    } else {
      return getTermExtents(byteWord);
    }
  }

  @Override
  public DiskExtentIterator getIterator(byte[] key) throws IOException {
    return getTermExtents(key);
  }

  private DiskCountIterator getTermCounts(byte[] term) throws IOException {
    PositionalPostingList postingList = postings.get(term);
    if (postingList != null) {
      return new DiskCountIterator(new MemoryPositionalIndexCountSource(postingList));
    }
    return null;
  }

  private DiskExtentIterator getTermExtents(byte[] term) throws IOException {
    PositionalPostingList postingList = postings.get(term);
    if (postingList != null) {
      return new DiskExtentIterator(new MemoryPositionalIndexExtentSource(postingList));
    }
    return null;
  }

  // try to free up memory.
  @Override
  public void close() throws IOException {
    postings = null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(DiskCountIterator.class));
    types.put("extents", new NodeType(DiskExtentIterator.class));
    return types;
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
    return collectionPostingsCount;
  }

  @Override
  public long getKeyCount() {
    return postings.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(p));

    KIterator kiterator = new KIterator();
    DiskExtentIterator viterator;
    ExtentArray extents;
    ScoringContext sc = new ScoringContext();
    while (!kiterator.isDone()) {
      viterator = kiterator.getValueIterator();
      writer.processWord(kiterator.getKey());

      while (!viterator.isDone()) {
        sc.document = viterator.currentCandidate();
        writer.processDocument(viterator.currentCandidate());
        extents = viterator.extents(sc);
        for (int i = 0; i < extents.size(); i++) {
          writer.processPosition(extents.begin(i));
          writer.processTuple();
        }
        viterator.movePast(viterator.currentCandidate());
      }
      kiterator.nextKey();
    }
    writer.close();
  }

  @Override
  public IndexPartStatistics getStatistics() {
    IndexPartStatistics is = new IndexPartStatistics();
    is.partName = "MemoryPositionIndex";
    is.collectionLength = this.collectionPostingsCount;
    is.vocabCount = this.vocabCount;
    is.highestDocumentCount = this.highestDocumentCount;
    is.highestFrequency = this.highestFrequency;
    return is;
  }

  // private functions
  private String stemAsRequired(String term) {
    if (stemmer != null) {
      return stemmer.stem(term);
    }
    return term;
  }

  // sub classes:
  public static class PositionalPostingList {

    byte[] key;
    CompressedByteBuffer documents_cbb = new CompressedByteBuffer();
    CompressedByteBuffer counts_cbb = new CompressedByteBuffer();
    CompressedByteBuffer positions_cbb = new CompressedByteBuffer();
    long termDocumentCount = 0;
    long termPostingsCount = 0;
    long maximumPostingsCount = 0;
    long lastDocument = 0;
    int lastCount = 0;
    int lastPosition = 0;

    public PositionalPostingList(byte[] key) {
      this.key = key;
    }

    public void add(long document, int position) {
      if (termDocumentCount == 0) {
        // first instance of term
        lastDocument = document;
        lastCount = 1;
        termDocumentCount += 1;
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
        termDocumentCount += 1;
        lastPosition = 0;
      }

      assert lastPosition == 0 || position > lastPosition;
      positions_cbb.add(position - lastPosition);
      termPostingsCount += 1;
      lastPosition = position;

      // keep track of the document with the highest frequency of 'term'
      maximumPostingsCount = Math.max(maximumPostingsCount, lastCount);
    }

    public byte[] key() {
      return key;
    }

    public byte[] getDocumentDataBytes() {
      return documents_cbb.getBytes();
    }

    public byte[] getCountDataBytes() {
      return counts_cbb.getBytes();
    }

    public byte[] getPositionDataBytes() {
      return positions_cbb.getBytes();
    }

    public long lastDocument() {
      return lastDocument;
    }

    public int lastCount() {
      return lastCount;
    }

    public NodeStatistics stats() {
      NodeStatistics stats = new NodeStatistics();
      stats.node = ByteUtil.toString(key);
      stats.nodeFrequency = termPostingsCount;
      stats.nodeDocumentCount = termDocumentCount;
      stats.maximumCount = maximumPostingsCount;
      return stats;
    }
  }
  // iterator allows for query processing and for streaming posting list data
  // public class Iterator extends ExtentIterator implements IndexIterator {

  public class KIterator implements KeyIterator {

    Iterator<byte[]> iterator;
    byte[] currKey;
    boolean done = false;

    public KIterator() throws IOException {
      iterator = postings.keySet().iterator();
      this.nextKey();
    }

    @Override
    public void reset() throws IOException {
      iterator = postings.keySet().iterator();
    }

    @Override
    public String getKeyString() throws IOException {
      return ByteUtil.toString(currKey);
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
      iterator = postings.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      iterator = postings.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public String getValueString() throws IOException {
      DiskExtentIterator it = getValueIterator();
      long count = it.totalEntries();
      StringBuilder sb = new StringBuilder();
      sb.append(ByteUtil.toString(getKey())).append(",");
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
    public DiskExtentIterator getValueIterator() throws IOException {
      if (currKey != null) {
        return new DiskExtentIterator(new MemoryPositionalIndexExtentSource(postings.get(currKey)));
      } else {
        return null;
      }
    }
  }
}
