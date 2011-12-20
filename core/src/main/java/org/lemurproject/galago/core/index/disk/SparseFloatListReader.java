// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.lemurproject.galago.core.index.GenericIndexReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.structured.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoreValueIterator;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Retrieves lists of floating point numbers which can be used as document features.
 * 
 * @author trevor
 */
public class SparseFloatListReader extends KeyListReader {

  public class KeyIterator extends KeyListReader.Iterator {

    public KeyIterator(GenericIndexReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      ListIterator it;
      long count = -1;
      try {
        it = new ListIterator(iterator);
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

    public ListIterator getValueIterator() throws IOException {
      return new ListIterator(iterator);
    }
  }

  public class ListIterator extends KeyListReader.ListIterator
          implements ScoreValueIterator {

    VByteInput stream;
    int documentCount;
    int index;
    int currentDocument;
    double currentScore;
    ScoringContext context;

    public ListIterator(GenericIndexReader.Iterator iterator) throws IOException {
      reset(iterator);
    }

    void read() throws IOException {
      index += 1;

      if (index < documentCount) {
        currentDocument += stream.readInt();
        currentScore = stream.readFloat();
      }
    }

    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentScore);

      return builder.toString();
    }

    public boolean next() throws IOException {
      read();
      if (!isDone()) {
        return true;
      }
      return false;
    }

    public void reset(GenericIndexReader.Iterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      documentCount = stream.readInt();
      index = -1;
      currentDocument = 0;
      if (documentCount > 0) {
        read();
      }
    }

    public void reset() throws IOException {
      throw new UnsupportedOperationException("This iterator does not reset without the parent KeyIterator.");
    }

    public void setContext(ScoringContext dc) {
      this.context = dc;
    }

    public ScoringContext getContext() {
      return context;
    }

    public int currentCandidate() {
      return currentDocument;
    }

    public boolean hasMatch(int document) {
      return document == currentDocument;
    }

    public boolean moveTo(int document) throws IOException {
      while (!isDone() && document > currentDocument) {
        read();
      }
      return hasMatch(document);
    }

    public void movePast(int document) throws IOException {
      while (!isDone() && document >= currentDocument) {
        read();
      }
    }

    public double score(ScoringContext dc) {
      if (currentDocument == dc.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
    }

    public double score() {
      if (currentDocument == context.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
    }

    public boolean isDone() {
      return index >= documentCount;
    }

    public long totalEntries() {
      return documentCount;
    }

    public double maximumScore() {
      return Double.POSITIVE_INFINITY;
    }

    public double minimumScore() {
      return Double.NEGATIVE_INFINITY;
    }

    public TObjectDoubleHashMap<String> parameterSweepScore() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public SparseFloatListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public ListIterator getListIterator() throws IOException {
    return new ListIterator(reader.getIterator());
  }

  public ListIterator getScores(String term) throws IOException {
    GenericIndexReader.Iterator iterator = reader.getIterator(Utility.fromString(term));
    return new ListIterator(iterator);
  }

  public void close() throws IOException {
    reader.close();
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("scores", new NodeType(Iterator.class));
    return nodeTypes;
  }

  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("scores")) {
      return getScores(node.getDefaultParameter());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }
}
