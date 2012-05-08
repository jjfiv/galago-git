// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.disk.SparseFloatListWriter;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Utility.ByteArrComparator;
import org.lemurproject.galago.tupleflow.VByteInput;


/*
 * author sjh
 *
 * In-memory posting index
 *
 */
public class MemorySparseFloatIndex implements MemoryIndexPart {

  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  protected TreeMap<byte[], PostingList> postings = new TreeMap(new ByteArrComparator());
  protected Parameters parameters;
  protected long collectionDocumentCount = 0;
  protected long collectionPostingsCount = 0;
  protected Stemmer stemmer = null;

  public MemorySparseFloatIndex(Parameters parameters) throws Exception {
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
    // - we have no way of extracting scores from documents at the moment
  }

  @Override
  public void addIteratorData(byte[] key, MovableIterator iterator) throws IOException {
    // if  we have not already cached this data
    if (!postings.containsKey(key)) {
      PostingList postingList = new PostingList(key);
      MovableScoreIterator mi = (MovableScoreIterator) iterator;
      ScoringContext c = (mi instanceof ContextualIterator) ? ((ContextualIterator) mi).getContext() : null;
      while (!mi.isDone()) {
        int document = mi.currentCandidate();
        if (c != null) {
          c.document = document;
          c.moveLengths(document);
        }

        double score = mi.score();
        postingList.add(document, score);
        mi.next();
      }

      // specifically wait until we have finished building the posting list to add it
      //  - we do not want to search partial data.
      postings.put(key, postingList);

      mi.reset();
    }
  }

  // Posting List Reader functions
  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    String stringKey = stemAsRequired(node.getDefaultParameter());
    byte[] key = Utility.fromString(stringKey);
    if (node.getOperator().equals("scores")) {
      return getNodeScores(key);
    }
    return null;
  }

  @Override
  public ValueIterator getIterator(byte[] key) throws IOException {
    return getNodeScores(key);
  }


  protected ScoresIterator getNodeScores(byte[] key) throws IOException {
    PostingList postingList = postings.get(key);
    if (postingList != null) {
      return new ScoresIterator(postingList);
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
    types.put("scores", new NodeType(ScoresIterator.class));
    return types;
  }

  @Override
  public String getDefaultOperator() {
    return "scores";
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
    ScoresIterator viterator;
    while (!kiterator.isDone()) {
      viterator = (ScoresIterator) kiterator.getValueIterator();
      writer.processWord(kiterator.getKey());

      while (!viterator.isDone()) {
        writer.processNumber(viterator.currentCandidate());
        writer.processTuple(viterator.score());
        viterator.next();
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
  public class PostingList {

    byte[] key;
    CompressedByteBuffer documents_cbb = new CompressedByteBuffer();
    CompressedByteBuffer scores_cbb = new CompressedByteBuffer();
    int termPostingsCount = 0;
    int lastDocument = 0;
    double maxScore = Double.MIN_VALUE;
    double minScore = Double.MAX_VALUE;

    public PostingList(byte[] key) {
      this.key = key;
    }

    public void add(int document, double score) {
      assert lastDocument == 0 || document > lastDocument : "Can not add documents in non-increasing order.";

      documents_cbb.add(document - lastDocument);
      lastDocument = document;

      maxScore = Math.max(maxScore, score);
      minScore = Math.min(minScore, score);

      scores_cbb.addFloat((float) score);

      termPostingsCount += 1;
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
      long count = -1;
      ScoresIterator it = new ScoresIterator(postings.get(currKey));
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKey())).append(",");
      sb.append("score:").append(it.score());
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
      if (currKey != null) {
        return new ScoresIterator(postings.get(currKey));
      } else {
        return null;
      }
    }
  }

  public class ScoresIterator extends ValueIterator implements
          MovableScoreIterator, ContextualIterator {

    PostingList postings;
    VByteInput documents_reader;
    VByteInput scores_reader;
    int iteratedDocs;
    int currDocument;
    double currScore;
    boolean done;
    ScoringContext context;
    Map<String, Object> modifiers;

    private ScoresIterator(PostingList postings) throws IOException {
      this.postings = postings;
      reset();
    }

    @Override
    public void reset() throws IOException {
      documents_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.documents_cbb.getBytes())));
      scores_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.scores_cbb.getBytes())));

      iteratedDocs = 0;
      currDocument = 0;
      currScore = 0;

      next();
    }

    @Override
    public double score() {
      return currScore;
    }

    @Override
    public double maximumScore() {
      return postings.maxScore;
    }

    @Override
    public double minimumScore() {
      return postings.minScore;
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
      if (iteratedDocs >= postings.termPostingsCount) {
        done = true;
        return;
      } else {
        currDocument += documents_reader.readInt();
        currScore = scores_reader.readFloat();
      }

      iteratedDocs++;
    }

    @Override
    public void moveTo(int identifier) throws IOException {
      // TODO: need to implement skip lists

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

      builder.append(Utility.toString(postings.key));
      builder.append(",");
      builder.append(currDocument);
      builder.append(",");
      builder.append(currScore);

      return builder.toString();
    }

    @Override
    public long totalEntries() {
      return postings.termPostingsCount;
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

    // This will pass up topdocs information if it's available
    @Override
    public void setContext(ScoringContext context) {
      this.context = context;
    }

    @Override
    public ScoringContext getContext() {
      return this.context;
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(postings.key);
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return postings.key;
    }
  }
}
