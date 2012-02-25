// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Superclass for any iterator that provides a direct mapping
 * of keys -> values, and we assume the entire value can be read into
 * memory.
 *
 * Implementations should provide their own methods for manipulating the keys and
 * values.
 * 
 * @author irmarc
 */
public abstract class KeyValueReader implements IndexPartReader {

  protected BTreeReader reader;

  public KeyValueReader(String filename) throws FileNotFoundException, IOException {
    reader = BTreeFactory.getBTreeReader(filename);
  }

  public Parameters getManifest() {
    return reader.getManifest();
  }

  public KeyValueReader(BTreeReader r) {
    this.reader = r;
  }

  public void close() throws IOException {
    reader.close();
  }

  public String getDefaultOperator() {
    Map<String, NodeType> types = this.getNodeTypes();
    if (types.size() == 1) {
      return types.keySet().toArray(new String[0])[0];
    } else {
      return reader.getManifest().get("defaultOperator", "none");
    }
  }

  public abstract Iterator getIterator() throws IOException;

  public static abstract class Iterator implements KeyIterator {

    protected BTreeReader.Iterator iterator;
    protected BTreeReader reader;

    public Iterator(BTreeReader reader) throws IOException {
      this.reader = reader;
      reset();
    }

    public boolean isDone() {
      return iterator.isDone();
    }

    public boolean skipToKey(byte[] key) throws IOException {
      iterator.skipTo(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        return true;
      }
      return false;
    }

    public boolean findKey(byte[] key) throws IOException {
      iterator.find(key);
      if (Utility.compare(key, iterator.getKey()) == 0) {
        return true;
      }
      return false;
    }

    public int compareTo(KeyIterator other) {
      try {
        return Utility.compare(getKeyBytes(), other.getKeyBytes());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean nextKey() throws IOException {
      return (iterator.nextKey());
    }

    public void reset() throws IOException {
      iterator = reader.getIterator();
    }

    public byte[] getValueBytes() throws IOException {
      return iterator.getValueBytes();
    }

    public DataStream getValueStream() throws IOException {
      return iterator.getValueStream();
    }

    public byte[] getKeyBytes() {
      return iterator.getKey();
    }

    public String getKey() {
      return Utility.toString(iterator.getKey());
    }

    public abstract String getValueString();
  }
}
