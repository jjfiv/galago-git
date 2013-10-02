// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskBooleanIterator;
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
    byte[] valueBytes = reader.getValueBytes(Utility.fromLong(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toBoolean(valueBytes);
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader, def);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("indicator", new NodeType(DiskBooleanIterator.class));
    return types;
  }

  @Override
  public DiskBooleanIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("indicator")) {
      boolean dflt = node.getNodeParameters().get("default", def);
      return new DiskBooleanIterator(new DocumentIndicatorSource(reader, dflt));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public static class KeyIterator extends KeyValueReader.KeyValueIterator {
    private final boolean def;

    public KeyIterator(BTreeReader reader, boolean def) throws IOException {
      super(reader);
      this.def = def;
    }

    @Override
    public String getKeyString() {
      return Long.toString(Utility.toLong(iterator.getKey()));
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
      return skipToKey(Utility.fromLong(key));
    }

    public int getCurrentDocument() {
      return (int) Utility.toLong(iterator.getKey());
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

    public DocumentIndicatorSource getValueSource() throws IOException {
      return new DocumentIndicatorSource(reader, def);
    }

    @Override
    public DiskBooleanIterator getValueIterator() throws IOException {
      return new DiskBooleanIterator(new DocumentIndicatorSource(reader, def));
    }
  }
}
