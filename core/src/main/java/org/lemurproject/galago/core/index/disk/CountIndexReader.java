// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskCountIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads a count based index structure mapping( term -> list(document-id),
 * list(document-freq) )
 *
 * Skip lists are supported
 *
 * @author sjh
 */
public class CountIndexReader extends KeyListReader implements AggregateIndexPart {

  Stemmer stemmer;

  public CountIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    stemmer = Stemmer.instance(reader.getManifest());
  }

  public CountIndexReader(String pathname) throws Exception {
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
  public DiskCountIterator getTermCounts(byte[] key) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(key);

    if (iterator != null) {
      return new DiskCountIterator(new CountIndexCountSource(iterator));
    }
    return null;
  }

  public DiskCountIterator getTermCounts(String term) throws IOException {
    return getTermCounts(Utility.fromString(stemmer.stemAsRequired(term)));
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(DiskCountIterator.class));
    return types;
  }

  @Override
  public BaseIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("counts")) {
      return getTermCounts(node.getDefaultParameter());
    }
    return null;
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
      CountIndexCountSource it;
      NodeStatistics ns = null;
      try {
        it = new CountIndexCountSource(iterator);
        ns = it.getStatistics();
      } catch (IOException ioe) {
        ioe.printStackTrace();
        System.err.println(ioe.toString());
      }

      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKey())).append(",");
      if (ns != null) {
        sb.append(ns.toString());
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    public CountIndexCountSource getStreamValueSource() throws IOException {
      return new CountIndexCountSource(iterator);
    }
    
    @Override
    public BaseIterator getValueIterator() throws IOException {
      return new DiskCountIterator(new CountIndexCountSource(iterator));
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }
  }
}
