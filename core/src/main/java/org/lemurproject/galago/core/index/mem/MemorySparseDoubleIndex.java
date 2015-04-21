// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.utility.buffer.CompressedByteBuffer;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.SparseFloatListWriter;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

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
public class MemorySparseDoubleIndex implements MemoryIndexPart {

  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  protected TreeMap<byte[], PostingList> postings = new TreeMap<>(new CmpUtil.ByteArrComparator());
  protected Parameters parameters;
  protected long collectionDocumentCount = 0;
  protected long collectionPostingsCount = 0;
  protected Stemmer stemmer = null;

  public MemorySparseDoubleIndex(Parameters parameters) throws Exception {
    this.parameters = parameters;

    if (parameters.containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(parameters.getString("stemmer")).newInstance();
    }

    // if the parameters specify a collection length use them.
    collectionPostingsCount = parameters.get("statistics/collectionLength", 0);
    collectionDocumentCount = parameters.get("statistics/documentCount", 0);
  }

  @Override
  public void addDocument(Document doc) throws IOException {
    // do nothing
    // - we have no methods of extracting scores from documents at the moment
  }

  @Override
  public void addIteratorData(byte[] key, BaseIterator iterator) throws IOException {
    // if  we have not already cached this data
    if (!postings.containsKey(key)) {


      ScoreIterator mi = (ScoreIterator) iterator;
      ScoringContext c = new ScoringContext();
      c.document = -1; // impossible document score - to extract defaulty score.

      // note that dirichet should not have a static default score
      //  -> this cache should not be used for dirichlet scores
      double defaultScore = Utility.tinyLogProbScore;
      PostingList postingList = new PostingList(key, defaultScore);

      while (!mi.isDone()) {
        long document = mi.currentCandidate();
        c.document = document;
        if (mi.hasMatch(c)) {
          double score = mi.score(c);
          postingList.add(document, score);
        }
        mi.movePast(document);
      }

      // specifically wait until we have finished building the posting list to add it
      //  - we do not want to search partial data.
      postings.put(key, postingList);

      mi.reset();
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    postings.remove(key);
  }

  // Posting List Reader functions
  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public DiskScoreIterator getIterator(Node node) throws IOException {
    String stringKey = stemAsRequired(node.getDefaultParameter());
    byte[] key = ByteUtil.fromString(stringKey);
    if (node.getOperator().equals("scores")) {
      return getNodeScores(key);
    }
    return null;
  }

  @Override
  public DiskScoreIterator getIterator(byte[] key) throws IOException {
    return getNodeScores(key);
  }

  protected DiskScoreIterator getNodeScores(byte[] key) throws IOException {
    PostingList postingList = postings.get(key);
    if (postingList != null) {
      return new DiskScoreIterator(new MemorySparseDoubleIndexScoreSource(postingList));
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
    types.put("scores", new NodeType(DiskScoreIterator.class));
    return types;
  }

  @Override
  public Parameters getManifest() {
    return parameters;
  }

  @Override
  public long getDocumentCount() {
    // doesn't work/make sense
    return collectionDocumentCount;
  }

  @Override
  public long getCollectionLength() {
    // doesn't work/make sense
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
    p.set("statistics/documentCount", this.getDocumentCount());
    p.set("statistics/collectionLength", this.getCollectionLength());
    p.set("statistics/vocabCount", this.getKeyCount());
    SparseFloatListWriter writer = new SparseFloatListWriter(new FakeParameters(p));

    KIterator kiterator = new KIterator();
    ScoreIterator viterator;
    ScoringContext sc = new ScoringContext();
    while (!kiterator.isDone()) {
      viterator = (ScoreIterator) kiterator.getValueIterator();
      writer.processWord(kiterator.getKey());
      while (!viterator.isDone()) {
        sc.document = viterator.currentCandidate();
        writer.processNumber(viterator.currentCandidate());
        writer.processTuple(viterator.score(sc));
        viterator.movePast(viterator.currentCandidate());
      }
      kiterator.nextKey();
    }
    writer.close();
  }

  // private functions
  private String stemAsRequired(String term) {
    if (stemmer != null) {
      return stemmer.stem(term);
    }
    return term;
  }

  // sub classes:
  public static class PostingList {

    byte[] key;
    CompressedByteBuffer documents_cbb = new CompressedByteBuffer();
    CompressedByteBuffer scores_cbb = new CompressedByteBuffer();
    long termPostingsCount = 0;
    long lastDocument = 0;
    double maxScore = Double.MIN_VALUE;
    double minScore = Double.MAX_VALUE;
    double defaultScore;

    public PostingList(byte[] key, double defaultScore) {
      this.key = key;
      this.defaultScore = defaultScore;
    }

    public void add(long document, double score) {
      assert lastDocument == 0 || document > lastDocument : "Can not add documents in non-increasing order.";

      documents_cbb.add(document - lastDocument);
      lastDocument = document;

      maxScore = Math.max(maxScore, score);
      minScore = Math.min(minScore, score);

      scores_cbb.addDouble(score);

      termPostingsCount += 1;
    }

    public byte[] key() {
      return key;
    }

    public byte[] getDocumentDataBytes() {
      return documents_cbb.getBytes();
    }

    public byte[] getScoreDataBytes() {
      return scores_cbb.getBytes();
    }

    public long lastDocument() {
      return lastDocument;
    }

    public long postingCount() {
      return termPostingsCount;
    }

    public double defaultScore() {
      return defaultScore;
    }

    public double minScore() {
      return minScore;
    }

    public double maxScore() {
      return maxScore;
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
      DiskScoreIterator it = getValueIterator();
      return ByteUtil.toString(getKey()) + "," + "entries:" + it.totalEntries();
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
        return CmpUtil.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public DiskScoreIterator getValueIterator() throws IOException {
      if (currKey != null) {
        return new DiskScoreIterator(new MemorySparseDoubleIndexScoreSource(postings.get(currKey)));
      } else {
        return null;
      }
    }
  }
}
