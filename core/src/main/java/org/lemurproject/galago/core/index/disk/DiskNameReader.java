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
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;

import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads a binary file of document names produced by DocumentNameWriter2
 * 
 * @author sjh
 */
public class DiskNameReader extends KeyValueReader implements NamesReader {

  /** Creates a new instance of DiskNameReader */
  public DiskNameReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReader(BTreeReader r) {
    super(r);
  }

  // gets the document name of the internal id index.
  @Override
  public String getDocumentName(int index) throws IOException {
    byte[] data = reader.getValueBytes(Utility.fromInt(index));
    if (data == null) {
      throw new IOException("Unknown Document Number : " + index);
    }
    return Utility.toString(data);
  }

  // gets the document id for some document name
  @Override
  public int getDocumentIdentifier(String documentName) throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc name -> doc int mappings");
  }

  @Override
  public NamesReader.Iterator getNamesIterator() throws IOException {
    return new ValueIterator(getIterator());
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public KeyToListIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return (KeyToListIterator) getNamesIterator();
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.Iterator {

    protected BTreeReader input;
    protected BTreeReader.Iterator iterator;

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(int identifier) throws IOException {
      return skipToKey(Utility.fromInt(identifier));
    }

    public String getCurrentName() throws IOException {
      return Utility.toString(getValueBytes());
    }

    public int getCurrentIdentifier() throws IOException {
      return Utility.toInt(getKeyBytes());
    }

    @Override
    public String getValueString() {
      try {
        return Utility.toString(getValueBytes());
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKey() {
      return Integer.toString(Utility.toInt(getKeyBytes()));
    }

    @Override
    public KeyToListIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.Iterator {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    @Override
    public String getEntry() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      StringBuilder sb = new StringBuilder();
      sb.append(ki.getCurrentIdentifier());
      sb.append(",");
      sb.append(ki.getCurrentName());
      return sb.toString();
    }

    @Override
    public long totalEntries() {
      return reader.getManifest().getLong("keyCount");
    }

    @Override
    public String getData() {
      try {
        return getEntry();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public boolean skipToKey(int candidate) throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.skipToKey(candidate);
    }

    @Override
    public String getCurrentName() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentName();
    }

    @Override
    public int getCurrentIdentifier() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentIdentifier();
    }
  }
}
