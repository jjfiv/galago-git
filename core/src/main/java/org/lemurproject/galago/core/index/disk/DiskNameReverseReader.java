// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;

import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads a binary file of document names produced by DocumentNameWriter2
 * 
 * @author sjh
 */
public class DiskNameReverseReader extends KeyValueReader implements NamesReader {

  /** Creates a new instance of DiskNameReader */
  public DiskNameReverseReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReverseReader(BTreeReader r) {
    super(r);
  }

  // gets the document name of the internal id index.
  public String getDocumentName(int index) throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
  }

  // gets the document id for some document name
  public int getDocumentIdentifier(String documentName) throws IOException {
    byte[] data = reader.getValueBytes(Utility.fromString(documentName));
    if (data == null) {
      throw new IOException("Unknown Document Name : " + documentName);
    }
    return Utility.toInt(data);
  }

  public NamesReader.Iterator getNamesIterator() throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
  }

  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    return types;
  }

  public KeyToListIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException(
            "Index doesn't support operator: " + node.getOperator());
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    protected BTreeReader input;
    protected BTreeReader.BTreeIterator iterator;

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(String name) throws IOException {
      return findKey(Utility.fromString(name));
    }

    public String getCurrentName() throws IOException {
      return Utility.toString(getKey());
    }

    public int getCurrentIdentifier() throws IOException {
      return Utility.toInt(getValueBytes());
    }

    public String getValueString() {
      try {
        return Integer.toString(Utility.toInt(getValueBytes()));
      } catch (IOException e) {
        return "Unknown";
      }
    }

    public String getKeyString() {
      return Utility.toString(getKey());
    }

    public KeyToListIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
    }
  }
}
