// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DocumentPriorReader extends KeyValueReader {

  private double def;
  protected Parameters manifest;

  public DocumentPriorReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    this.manifest = this.reader.getManifest();
    def = this.getManifest().get("default", Math.log(0.0000000001)); // this must exist
  }

  public DocumentPriorReader(BTreeReader r) {
    super(r);
    this.manifest = this.reader.getManifest();
    def = this.getManifest().get("default", Math.log(0.0000000001)); // this must exist
  }

  public double getPrior(int document) throws IOException {
    byte[] valueBytes = reader.getValueBytes(Utility.fromInt(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toDouble(valueBytes);
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("prior", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("prior")) {
      return new ValueIterator(new KeyIterator(reader), node);
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKeyString() {
      return Integer.toString(getCurrentDocument());
    }

    @Override
    public String getValueString() {
      try {
        return Double.toString(getCurrentScore());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromInt(key));
    }

    public int getCurrentDocument() {
      return Utility.toInt(iterator.getKey());
    }

    public double getCurrentScore() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toDouble(valueBytes);
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(new KeyIterator(reader));
    }
  }

  // needs to be an AbstractIndicator
  public class ValueIterator extends KeyToListIterator implements MovableScoreIterator {

    double minScore;

    public ValueIterator(KeyIterator it, Node node) {
      super(it);
      this.minScore = node.getNodeParameters().get("defaultPrior", def); // same as indri
    }

    public ValueIterator(KeyIterator it) {
      super(it);
      this.minScore = def; // same as indri
    }

    @Override
    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getValueString();
    }

    @Override
    public long totalEntries() {
      return manifest.get("keyCount", -1);
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public boolean hasMatch(int identifier) {
      return true;
    }

    @Override
    public double score() {
      try {
        if (this.currentCandidate() == context.document) {
          byte[] valueBytes = iterator.getValueBytes();
          if ((valueBytes == null) || (valueBytes.length == 0)) {
            return minScore;
          } else {
            return Utility.toDouble(valueBytes);
          }
        } else {
          return minScore;
        }
      } catch (IOException ex) {
        Logger.getLogger(DocumentPriorReader.class.getName()).log(Level.SEVERE, null, ex);
        throw new RuntimeException(ex);
      }
    }

    @Override
    public int currentCandidate() {
      try {
        return Utility.toInt(this.iterator.getKey());
      } catch (IOException ex) {
        throw new RuntimeException("Prior Reader - failed to convert index.getKeyBytes to an integer.");
      }
    }

    @Override
    public double maximumScore() {
      return manifest.get("maxScore", Double.POSITIVE_INFINITY);
    }

    @Override
    public double minimumScore() {
      return manifest.get("minScore", Double.NEGATIVE_INFINITY);
    }

    @Override
    public String getKeyString() throws IOException {
      return "priors";
    }

    @Override
    public byte[] getKeyBytes() throws IOException {
      return Utility.fromString("priors");
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "score";
      String className = DocumentPriorReader.class.getSimpleName();
      String parameters = "";
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Double.toString(score());
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
