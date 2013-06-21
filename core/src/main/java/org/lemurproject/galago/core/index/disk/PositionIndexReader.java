// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.retrieval.iterator.disk.StreamExtentIterator;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
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
 * (12/16/2010, irmarc): In order to facilitate faster count-only processing,
 * the default iterator created will not even open the positions list when
 * iterating. This is an interesting enough change that there are now two
 * versions of the iterator
 *
 * @author trevor, sjh, irmarc
 */
public class PositionIndexReader extends KeyListReader implements AggregateIndexPart {
  Stemmer stemmer = null;

  public PositionIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  public PositionIndexReader(String pathname) throws Exception {
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
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public StreamExtentIterator getTermExtents(String term) throws IOException {
    return getTermExtents(Utility.fromString(stemAsRequired(term)));
  }

  public StreamExtentIterator getTermExtents(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);
    if (iterator != null) {
      return new StreamExtentIterator(new StreamExtentSource(iterator));
    }
    return null;
  }

  public TermCountIterator getTermCounts(String term) throws IOException {
    return getTermCounts(Utility.fromString(stemAsRequired(term)));
  }

  public TermCountIterator getTermCounts(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);
    if (iterator != null) {
      return new TermCountIterator(iterator);
    }
    return null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(TermCountIterator.class));
    types.put("extents", new NodeType(StreamExtentIterator.class));
    return types;
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      return getTermCounts(node.getDefaultParameter());
    } else {
      return getTermExtents(node.getDefaultParameter());
    }
  }

  private String stemAsRequired(String term) {
    if (stemmer != null) {
      if (term.contains("~")) {
        return stemmer.stemWindow(term);
      } else {
        return stemmer.stem(term);
      }
    }
    return term;
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
  
  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
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
      sb.append(Utility.toString(getKey())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }
    
    public StreamExtentSource getValueSource() throws IOException {
      return new StreamExtentSource(iterator);
    }

    @Override
    public DiskIterator getValueIterator() throws IOException {
      return new StreamExtentIterator(getValueSource());
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }
}

