// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.btree.format.BTreeReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskScoreIterator;
import org.lemurproject.galago.utility.Parameters;
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
    byte[] valueBytes = reader.getValueBytes(Utility.fromLong(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toDouble(valueBytes);
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader, def);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("prior", new NodeType(DiskScoreIterator.class));
    return types;
  }

  @Override
  public DiskScoreIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("prior")) {
      return new DiskScoreIterator(new DocumentPriorSource(reader, def));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public static class KeyIterator extends KeyValueReader.KeyValueIterator {
    private final double def;

    public KeyIterator(BTreeReader reader, double def) throws IOException {
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
        return Double.toString(getCurrentScore());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromLong(key));
    }

    public int getCurrentDocument() {
      // TODO stop casting document to int
      return (int) Utility.toLong(iterator.getKey());
    }

    public double getCurrentScore() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toDouble(valueBytes);
      }
    }

    public DocumentPriorSource getValueSource() throws IOException {
      return new DocumentPriorSource(reader, def);
    }

    @Override
    public DiskScoreIterator getValueIterator() throws IOException {
      return new DiskScoreIterator(new DocumentPriorSource(reader, def));
    }
  }
}