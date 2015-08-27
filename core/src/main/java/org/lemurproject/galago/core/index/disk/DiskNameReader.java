// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads a binary file of document names produced by DiskNameWriter
 *
 * @author sjh
 */
public class DiskNameReader extends KeyValueReader implements NamesReader {

  /**
   * Creates a new create of DiskNameReader
   */
  public DiskNameReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReader(BTreeReader r) {
    super(r);
  }

  // gets the document name of the internal id index.
  @Override
  public String getDocumentName(long index) throws IOException {
    byte[] data = reader.getValueBytes(Utility.fromLong(index));
    if (data == null) {
      return null;
    }
    return ByteUtil.toString(data);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(DiskDataIterator.class));
    return types;
  }

  @Override
  public DiskDataIterator<String> getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return getNamesIterator();
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public DiskNameSource getSource() throws IOException {
    return new DiskNameSource(reader);
  }

  @Override
  public DiskDataIterator<String> getNamesIterator() throws IOException {
    return new DiskDataIterator<String>(new DiskNameSource(reader));
  }

  public static class KeyIterator extends KeyValueReader.KeyValueIterator {

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(int identifier) throws IOException {
      return skipToKey(Utility.fromLong(identifier));
    }

    public String getCurrentName() throws IOException {
      return ByteUtil.toString(getValueBytes());
    }

    public long getCurrentIdentifier() throws IOException {
      return Utility.toLong(getKey());
    }

    @Override
    public String getValueString() {
      try {
        return ByteUtil.toString(getValueBytes());
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKeyString() {
      return Long.toString(Utility.toLong(getKey()));
    }

    @Override
    public DiskDataIterator<String> getValueIterator() throws IOException {
      return new DiskDataIterator<String>(new DiskNameSource(reader));
    }
  }
}
