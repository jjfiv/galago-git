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
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * 
 * @author sjh
 * @author irmarc
 */
public class DocumentIndicatorReader extends KeyValueReader {

  protected boolean def;
  protected Parameters manifest;

  public DocumentIndicatorReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    this.manifest = this.reader.getManifest();
    def = this.manifest.get("default", false);  // Play conservative
  }

  public DocumentIndicatorReader(BTreeReader r) {
    super(r);
  }

  public boolean getIndicator(int document) throws IOException {
    byte[] valueBytes = reader.getValueBytes(Utility.fromInt(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toBoolean(valueBytes);
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("indicator", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("indicator")) {
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
        return Boolean.toString(getCurrentIndicator());
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

    public boolean getCurrentIndicator() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toBoolean(valueBytes);
      }
    }

    @Override
    public boolean isDone() {
      return iterator.isDone();
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  // needs to be an AbstractIndicator
  public class ValueIterator extends KeyToListIterator implements IndicatorIterator {

    boolean defInst;

    public ValueIterator(KeyIterator it, Node node) {
      super(it);
      this.defInst = node.getNodeParameters().get("default", def); // same as indri
    }

    public ValueIterator(KeyIterator it) {
      super(it);
      this.defInst = def; // same as indri
    }

    @Override
    public String getEntry() throws IOException {
      return Integer.toString(((KeyIterator) iterator).getCurrentDocument());
    }

    @Override
    public long totalEntries() {
      return manifest.get("keyCount", -1);
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }
    
    @Override
    public boolean hasMatch(int document) {
      return super.hasMatch(document) && indicator(document);
    }
    
    @Override
    public boolean indicator(int document) {
      if (document == currentCandidate()) {
        try {
          return ((KeyIterator) iterator).getCurrentIndicator();
        } catch (IOException ex) {
          Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ex);
          throw new RuntimeException("Failed to read indicator file.");
        }
      }
      return this.defInst;
    }

    @Override
    public String getKeyString() throws IOException {
      return "indicators";
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "indicator";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = Boolean.toString(indicator(this.context.document));
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
