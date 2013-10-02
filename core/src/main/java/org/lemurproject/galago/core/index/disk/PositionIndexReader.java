// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.SourceIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads a simple positions-based index, where each inverted list in the index
 * contains both term count information and term position information. The term
 * counts data is stored separately from term position information for faster
 * query processing when no positions are needed.
 *
 * @author trevor, sjh, irmarc
 */
public class PositionIndexReader extends KeyListReader implements AggregateIndexPart {

  Stemmer stemmer;

  public PositionIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    stemmer = Stemmer.instance(reader.getManifest());
  }

  public PositionIndexReader(String pathname) throws Exception {
    super(pathname);
    stemmer = Stemmer.instance(reader.getManifest());
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public DiskExtentIterator getTermExtents(String term) throws IOException {
    return getTermExtents(Utility.fromString(stemmer.stemAsRequired(term)));
  }

  public DiskExtentIterator getTermExtents(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);
    if (iterator != null) {
      return new DiskExtentIterator(new PositionIndexExtentSource(iterator));
    }
    return null;
  }

  public DiskCountIterator getTermCounts(String term) throws IOException {
    return getTermCounts(Utility.fromString(stemmer.stemAsRequired(term)));
  }

  public DiskCountIterator getTermCounts(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);
    if (iterator != null) {
      return new DiskCountIterator(new PositionIndexCountSource(iterator));
    }
    return null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(DiskCountIterator.class));
    types.put("extents", new NodeType(DiskExtentIterator.class));
    return types;
  }

  @Override
  public SourceIterator getIterator(Node node) throws IOException {
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
    is.partName = manifest.get("filename", "PositionIndexPart");
    return is;
  }

  // subclasses 
  public static class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      DiskCountIterator it;
      long count = -1;
      try {
        it = new DiskCountIterator(new PositionIndexCountSource(iterator));
        count = it.totalEntries();
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
    public DiskExtentIterator getValueIterator() throws IOException {
      return new DiskExtentIterator(new PositionIndexExtentSource(iterator));
    }

    public PositionIndexExtentSource getValueSource() throws IOException {
      return new PositionIndexExtentSource(iterator);
    }

    public PositionIndexCountSource getValueCountSource() throws IOException {
      return new PositionIndexCountSource(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }
}
