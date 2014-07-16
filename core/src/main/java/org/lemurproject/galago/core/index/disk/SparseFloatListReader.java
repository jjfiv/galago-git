// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskScoreIterator;
import org.lemurproject.galago.utility.ByteUtil;

/**
 * Retrieves lists of floating point numbers which can be used as document
 * features.
 *
 * @author trevor
 */
public class SparseFloatListReader extends KeyListReader {

  private static double defaultScore = Math.log(Math.pow(10, -10));

  public SparseFloatListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  private DiskScoreIterator getScores(String term, double defaultScore) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(ByteUtil.fromString(term));
    return new DiskScoreIterator(new SparseFloatListSource(iterator, defaultScore));
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("scores", new NodeType(DiskScoreIterator.class));
    return nodeTypes;
  }

  @Override
  public DiskScoreIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("scores")) {
      return getScores(node.getDefaultParameter(), node.getNodeParameters().get("defaultScore", defaultScore));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public static class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      ScoreIterator it;
      long count = -1;
      try {
        it = new DiskScoreIterator(new SparseFloatListSource(iterator, defaultScore));
        count = it.totalEntries();
      } catch (IOException ioe) {
      }

      StringBuilder sb = new StringBuilder();
      sb.append(ByteUtil.toString(iterator.getKey())).append(", List Value: size=");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public DiskScoreIterator getValueIterator() throws IOException {
      return new DiskScoreIterator(new SparseFloatListSource(iterator, defaultScore));
    }

    @Override
    public String getKeyString() throws IOException {
      return ByteUtil.toString(iterator.getKey());
    }

    public SparseFloatListSource getValueSource() throws IOException {
      return new SparseFloatListSource(iterator, defaultScore);
    }
  }
}
