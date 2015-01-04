// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index.disk;

import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.compression.VByte;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
  public BaseIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      String stem = stemAsRequired(node.getDefaultParameter());
      KeyIterator ki = new KeyIterator(reader);
      ki.findKey(ByteUtil.fromString(stem));
      if (CmpUtil.compare(ki.getKey(), ByteUtil.fromString(stem)) == 0) {
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

  public static class KeyIterator extends KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    public NodeStatistics getNodeStatistics() {
      try {
        DataInput value = iterator.getValueStream();

        NodeStatistics stats = new NodeStatistics();
        stats.node = getKeyString();
        stats.nodeFrequency = VByte.uncompressLong(value);
        stats.nodeDocumentCount = VByte.uncompressLong(value);
        stats.maximumCount = VByte.uncompressLong(value);

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
    public BaseIterator getValueIterator() throws IOException {
      return new BackgroundStatsIterator(this);
    }

    @Override
    public String getKeyString() throws IOException {
      return ByteUtil.toString(this.iterator.getKey());
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      return this.iterator.getValueBytes();
    }
  }

  public static class BackgroundStatsIterator implements BaseIterator, NodeAggregateIterator {

    protected KeyIterator iterator;

    public BackgroundStatsIterator(KeyIterator ki) {
      this.iterator = ki;
    }

    @Override
    public NodeStatistics getStatistics() {
      return this.iterator.getNodeStatistics();
    }

    public String getKeyString() throws IOException {
      return this.iterator.getKeyString();
    }

    // Nothing else works - this is not a normal iterator.
    @Override
    public long currentCandidate() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasMatch(long identifier) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void syncTo(long identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void movePast(long identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getValueString(ScoringContext sc) throws IOException {
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
    public int compareTo(BaseIterator o) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
