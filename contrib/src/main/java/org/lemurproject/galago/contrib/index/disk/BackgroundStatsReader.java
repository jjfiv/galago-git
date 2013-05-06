// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index.disk;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
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
public class BackgroundStatsReader extends KeyValueReader implements AggregateIndexPart {

  protected Parameters manifest;
  protected Stemmer stemmer;

  public BackgroundStatsReader(String filename) throws Exception {
    super(filename);
    this.manifest = this.reader.getManifest();
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  public BackgroundStatsReader(BTreeReader r) throws Exception {
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
    types.put("counts", new NodeType(BackgroundStatsIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      String stem = stemAsRequired(node.getDefaultParameter());
      KeyIterator ki = new KeyIterator(reader);
      ki.findKey(Utility.fromString(stem));
      if (Utility.compare(ki.getKey(), Utility.fromString(stem)) == 0) {
        return new BackgroundStatsIterator(ki);
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
    is.highestFrequency = manifest.get("statistics/highestCollectionFrequency", 0);
    is.partName = manifest.get("filename", "BackgroundLMPart");
    return is;
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    public NodeStatistics getNodeStatistics() {
      try {
        DataInput value = iterator.getValueStream();

        NodeStatistics stats = new NodeStatistics();
        stats.node = getKeyString();
        stats.nodeFrequency = Utility.uncompressLong(value);
        stats.nodeDocumentCount = Utility.uncompressLong(value);
        stats.maximumCount = Utility.uncompressLong(value);

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
      return new BackgroundStatsIterator(this);
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

  public class BackgroundStatsIterator extends ValueIterator implements NodeAggregateIterator {

    protected KeyIterator iterator;

    public BackgroundStatsIterator(KeyIterator ki) {
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
