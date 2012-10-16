// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIndexPart;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeAggregateIterator;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class BackgroundLMReader extends KeyValueReader implements AggregateIndexPart {

  protected Parameters manifest;
  protected Stemmer stemmer;

  public BackgroundLMReader(String filename) throws Exception {
    super(filename);
    this.manifest = this.reader.getManifest();
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  public BackgroundLMReader(BTreeReader r) throws Exception {
    super(r);
    this.manifest = this.reader.getManifest();
    if (manifest.containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(manifest.getString("stemmer")).newInstance();
    }
  }

  @Override
  public KeyValueIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(BackgroundLMIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      String stem = stemAsRequired(node.getDefaultParameter());
      KeyIterator ki = new KeyIterator(reader);
      ki.findKey(Utility.fromString(stem));
      if (Utility.compare(ki.getKey(), Utility.fromString(stem)) == 0) {
        return new BackgroundLMIterator(ki);
      }
      return null;
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  private String stemAsRequired(String term) {
    if (stemmer != null) {
      return stemmer.stem(term);
    }
    return term;
  }

  @Override
  public IndexPartStatistics getStatistics() {
    IndexPartStatistics is = new IndexPartStatistics();
    is.collectionLength = manifest.get("statistics/collectionLength", 0);
    is.vocabCount = manifest.get("statistics/vocabCount", 0);
    is.highestDocumentCount = manifest.get("statistics/highestDocumentCount", 0);
    is.highestFrequency = manifest.get("statistics/highestFrequency", 0);
    is.partName = manifest.get("filename", "BackgroundLMPart");
    return is;
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    long collectionLength;
    long documentCount;

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
      this.collectionLength = reader.getManifest().get("statistics/collectionLength", 1);
      this.documentCount = reader.getManifest().get("statistics/documentCount", 1);
    }

    public NodeStatistics getNodeStatistics() {
      try {
        DataInput value = iterator.getValueStream();

        NodeStatistics stats = new NodeStatistics();
        stats.node = getKeyString();
        stats.nodeFrequency = Utility.uncompressLong(value);
        stats.nodeDocumentCount = Utility.uncompressLong(value);
        stats.maximumCount = Integer.MAX_VALUE;
          
        return stats;
      } catch (IOException e) {
        throw new RuntimeException("Failed to collect statistics in BackgroundLMReader. " + e.getMessage());
      }
    }

    @Override
    public String getValueString() {
      return getNodeStatistics().toString();
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new BackgroundLMIterator(this);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(this.iterator.getKey());
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      return this.iterator.getValueBytes();
    }
  }

  public class BackgroundLMIterator extends ValueIterator implements
          NodeAggregateIterator, MovableCountIterator {

    protected KeyIterator iterator;

    public BackgroundLMIterator(KeyIterator ki) {
      this.iterator = ki;
    }

    public NodeStatistics getStatistics() {
      return this.iterator.getNodeStatistics();
    }

    @Override
    public String getKeyString() throws IOException {
      return this.iterator.getKeyString();
    }

    @Override
    public byte[] key() {
      return this.iterator.getKey();
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return this.iterator.getKey();
    }

    // Nothing else works - this is not a normal iterator.
    @Override
    public int currentCandidate() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasMatch(int identifier) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void syncTo(int identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void movePast(int identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getEntry() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isDone() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int count() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int maximumCount() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasAllCandidates() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(MovableIterator o) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AnnotatedNode getAnnotatedNode() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
