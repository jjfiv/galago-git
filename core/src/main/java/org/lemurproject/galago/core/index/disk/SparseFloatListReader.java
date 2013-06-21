// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskScoreIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Retrieves lists of floating point numbers which can be used as document
 * features.
 *
 * @author trevor
 */
public class SparseFloatListReader extends KeyListReader {

  private static double defaultScore = Math.log( Math.pow(10, -10) );
  
  public SparseFloatListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public DiskScoreIterator getListIterator() throws IOException {
    return new DiskScoreIterator(new SparseFloatListSource(reader.getIterator(), defaultScore));
  }

  private DiskScoreIterator getScores(String term, double defaultScore) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(Utility.fromString(term));
    return new DiskScoreIterator(new SparseFloatListSource(iterator, defaultScore));
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("scores", new NodeType(DiskScoreIterator.class));
    return nodeTypes;
  }

  @Override
  public DiskIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("scores")) {
      return getScores(node.getDefaultParameter(), node.getNodeParameters().get("defaultScore", defaultScore));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

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
      sb.append(Utility.toString(iterator.getKey())).append(", List Value: size=");
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
      return Utility.toString(iterator.getKey());
    }
  }

  @Deprecated
  public class ListIterator extends BTreeValueIterator
          implements ScoreIterator {

    VByteInput stream;
    int documentCount;
    int index;
    int currentDocument;
    double currentScore;
    double def;

    public ListIterator(BTreeReader.BTreeIterator iterator, double defaultScore) throws IOException {
      super(iterator.getKey());
      reset(iterator);
      def = defaultScore;
    }

    void read() throws IOException {
      index += 1;

      if (index < documentCount) {
        currentDocument += stream.readInt();
        currentScore = stream.readFloat();
      } else {
        // ensure we never overflow
        index = documentCount;
      }
    }

    @Override
    public String getValueString() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(getKeyString());
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentScore);

      return builder.toString();
    }

    @Override
    public void reset(BTreeReader.BTreeIterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      documentCount = stream.readInt();
      index = -1;
      currentDocument = 0;
      if (documentCount > 0) {
        read();
      }
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("This iterator does not reset without the parent KeyIterator.");
    }

    @Override
    public int currentCandidate() {
      return currentDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public void movePast(int document) throws IOException {
      while (!isDone() && document >= currentDocument) {
        read();
      }
    }

    @Override
    public void syncTo(int document) throws IOException {
      while (!isDone() && document > currentDocument) {
        read();
      }
    }

    @Override
    public double score() {
      if (currentDocument == context.document) {
        return currentScore;
      }
      return def;
    }

    @Override
    public boolean isDone() {
      return index >= documentCount;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public double maximumScore() {
      return Double.POSITIVE_INFINITY;
    }

    @Override
    public double minimumScore() {
      return Double.NEGATIVE_INFINITY;
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "scores";
      String className = this.getClass().getSimpleName();
      String parameters = this.getKeyString();
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Double.toString(score());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
