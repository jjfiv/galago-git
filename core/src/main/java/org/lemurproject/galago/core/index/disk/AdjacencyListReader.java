// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeReader.BTreeIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Stores a mapping from string -> [(int,double)] 
 * 
 * Used for...
 * 
 * @author irmarc
 */
public class AdjacencyListReader extends KeyListReader {

  public AdjacencyListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  public AdjacencyListReader(BTreeReader reader) {
    super(reader);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("neighbors", new NodeType(IntegerListIterator.class));
    return nodeTypes;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("neighbors")) {
      return getScores(node.getDefaultParameter());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  
  protected ValueIterator getScores(String term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(Utility.fromString(term));
    if (iterator != null) {
      return new IntegerListIterator(iterator);
    }
    return null;
  }

  
  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }

    @Override
    public String getValueString() {
      IntegerListIterator it;
      long count = -1;
      try {
        it = new IntegerListIterator(iterator);
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
    public ValueIterator getValueIterator() throws IOException {
      return new IntegerListIterator(iterator);
    }
  }

  public class IntegerListIterator extends KeyListReader.ListIterator
          implements MovableScoreIterator {

    VByteInput stream;
    int neighborhood;
    int index;
    int currentIdentifier;
    double currentScore;
    ScoringContext context;

    public IntegerListIterator(BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      reset(iterator);
    }

    @Override
    public void reset(BTreeIterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      neighborhood = stream.readInt();
      index = -1;
      key = iterator.getKey();
      currentIdentifier = 0;
      if (neighborhood > 0) {
        read();
      }
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void moveTo(int identifier) throws IOException {
      while (!isDone() && identifier > currentIdentifier) {
        read();
      }
    }

    @Override
    public void next() throws IOException {
      read();
    }

    @Override
    public int currentCandidate() {
      return currentIdentifier;
    }

    @Override
    public boolean hasAllCandidates(){
      return false;
    }
    
    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentIdentifier);
      builder.append(",");
      builder.append(currentScore);

      return builder.toString();
    }

    @Override
    public long totalEntries() {
      return neighborhood;
    }

    @Override
    public boolean isDone() {
      return index >= neighborhood;
    }

    @Override
    public double score() {
      if (currentIdentifier == context.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
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
    public void setContext(ScoringContext context) {
      this.context = context;
    }

    // private functions:
    private void read() throws IOException {
      index += 1;

      if (index < neighborhood) {
        currentIdentifier += stream.readInt();
        currentScore = stream.readDouble();
      }
    }
  }
}
