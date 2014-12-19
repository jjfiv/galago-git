// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.btree.format.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.SourceIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

/**
 * Reads a simple positions-based index, where each inverted list in the index
 * contains both term count information and term position information. The term
 * counts data is stored separately from term position information for faster
 * query processing when no positions are needed.
 *
 * @author sjh, irmarc,
 */
public class WindowIndexReader extends KeyListReader implements AggregateIndexPart {

  Stemmer stemmer;

  public WindowIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    stemmer = Stemmer.create(reader.getManifest());
  }

  public WindowIndexReader(String pathname) throws Exception {
    super(pathname);
    stemmer = Stemmer.create(reader.getManifest());
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
    term = stemmer.stemAsRequired(term);
    BTreeReader.BTreeIterator iterator = reader.getIterator(ByteUtil.fromString(term));
    if (iterator != null) {
      return new DiskExtentIterator(new WindowIndexExtentSource(iterator));
    }
    return null;
  }

  public DiskCountIterator getTermCounts(String term) throws IOException {
    term = stemmer.stemAsRequired(term);
    BTreeReader.BTreeIterator iterator = reader.getIterator(ByteUtil.fromString(term));

    if (iterator != null) {
      return new DiskCountIterator(new WindowIndexCountSource(iterator));
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
    is.partName = manifest.get("filename", "CountIndexPart");
    return is;
  }

  public static class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      DiskCountIterator it;
      long count = -1;
      try {
        it = new DiskCountIterator(new WindowIndexCountSource(iterator));
        count = it.totalEntries();
      } catch (IOException ioe) {
      }
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
    public DiskExtentIterator getValueIterator() throws IOException {
      return new DiskExtentIterator(new WindowIndexExtentSource(iterator));
    }

    public DiskCountIterator getCountValueIterator() throws IOException {
      return new DiskCountIterator(new WindowIndexCountSource(iterator));
    }

    public WindowIndexExtentSource getValueSource() throws IOException {
      return new WindowIndexExtentSource(iterator);
    }

    public WindowIndexCountSource getCountValueSource() throws IOException {
      return new WindowIndexCountSource(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return ByteUtil.toString(iterator.getKey());
    }
  }
}
