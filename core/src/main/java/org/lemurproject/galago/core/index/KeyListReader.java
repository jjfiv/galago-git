// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.iterator.ModifiableIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Base class for any data structures that map a key value
 * to a list of data, where one cannot assume the list can be
 * held in memory
 *
 *
 * @author irmarc
 */
public abstract class KeyListReader extends KeyValueReader {

  public KeyListReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public KeyListReader(GenericIndexReader r) {
    super(r);
  }

  public abstract class ListIterator implements ValueIterator, ModifiableIterator {

    // OPTIONS
    public static final int HAS_SKIPS = 0x01;
    public static final int HAS_MAXTF = 0x02;

    protected GenericIndexReader.Iterator source;
    protected byte[] key;
    protected long dataLength;
    protected Map<String, Object> modifiers = null;

    public abstract String getEntry();

    public int compareTo(ValueIterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return currentCandidate() - other.currentCandidate();
    }

    public void addModifier(String k, Object m) {
      if (modifiers == null) modifiers = new HashMap<String, Object>();
      modifiers.put(k, m);
    }

    public Set<String> getAvailableModifiers() {
      return modifiers.keySet();
    }

    public boolean hasModifier(String key) {
      return ((modifiers != null) && modifiers.containsKey(key));
    }

    public Object getModifier(String modKey) {
      if (modifiers == null) return null;
      return modifiers.get(modKey);
    }

    public long getByteLength() throws IOException {
      return dataLength;
    }

    public String getKey() {
      return Utility.toString(key);
    }

    public byte[] getKeyBytes() {
      return key;
    }

    public boolean hasMatch(int id) {
      return (!isDone() && currentCandidate() == id);
    }

    public void movePast(int id) throws IOException {
      moveTo(id + 1);
    }

    public abstract boolean moveTo(int id) throws IOException;

    public abstract void reset(GenericIndexReader.Iterator it) throws IOException;
  }
}
