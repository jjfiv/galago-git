// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.disk.DiskBTreeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.ReadOnlyMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jfoley
 */
public class DiskMapReader extends ReadOnlyMap<byte[], byte[]> implements Closeable {
	private static final Logger LOG = Logger.getLogger(DiskMapReader.class.getName());

  public final BTreeReader btree;
  public final Parameters opts;
    
  public DiskMapReader(String path) throws IOException {
    this.btree = new DiskBTreeReader(path);
    this.opts = btree.getManifest();
  }

  @Override
  public int size() {
    return (int) opts.getLong("keyCount");
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }
  
  private BTreeReader.BTreeIterator getIter(byte[] key) {
    try {
      return btree.getIterator(key);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }
  
  @Override
  public boolean containsKey(Object o) {
    if(o == null)
      return false;
    if(o instanceof String)
      return containsKey(ByteUtil.fromString((String) o));
    
    try {
      byte[] key = (byte[]) o;
      BTreeReader.BTreeIterator iter = getIter(key);
      if(iter == null || iter.getKey() == null)
        return false;
      return Arrays.equals(key, iter.getKey());
    } catch (ClassCastException ex) {
      return false;
    }
  }
  
  @Override
  public Set<byte[]> keySet() {
    Set<byte[]> keys = new TreeSet<>(new CmpUtil.ByteArrComparator());
    try {
      BTreeReader.BTreeIterator iter = btree.getIterator();
      while(!iter.isDone()) {
        keys.add(iter.getKey());
        iter.nextKey();
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return keys;
  }

  @Override
  public byte[] get(Object o) {
    if(o == null)
      return null;
    try {
      byte[] key = (byte[]) o;
      BTreeReader.BTreeIterator iter = getIter(key);
      if(iter != null && Arrays.equals(key, iter.getKey())) {
        return iter.getValueBytes();
      }
    } catch (ClassCastException ex) {
      // not found
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return null;
  }

  @Override
  public boolean containsValue(Object o) {
    throw new UnsupportedOperationException("This seems slow.");
  }
  
  @Override
  public Collection<byte[]> values() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Set<Entry<byte[], byte[]>> entrySet() {
    HashSet<Entry<byte[], byte[]>> entries = new HashSet<>();
    for(byte[] key : keySet()) {
      entries.add(new DiskMapReaderEntry(this, key));
    }
    return entries;
  }
  
  public static DiskMapReader fromMap(String path, Map<byte[], byte[]> other) throws IOException {
    LOG.log(Level.INFO, "Creating DiskMap at {0}", path);
    
    DiskMapSortedBuilder mb = new DiskMapSortedBuilder(path, Parameters.instance());
    
    ArrayList<byte[]> keys = new ArrayList<>(other.keySet());
    Collections.sort(keys, new CmpUtil.ByteArrComparator());
    
    for(byte[] key : keys) {
      mb.put(key, other.get(key));
    }
    mb.close();
    
    return new DiskMapReader(path);    
  }

  @Override
  public void close() throws IOException {
    this.btree.close();
  }

  /**
	 * This class allows you to iterate over all the entries, paying only to look up values you're interested in. Unfortunately, it charges log(n) for every .getValue() call. It doesn't and can't easily hide iterators inside.
	 */
  public static final class DiskMapReaderEntry implements Entry<byte[], byte[]> {
    private final DiskMapReader owner;
    private final byte[] key;
    public DiskMapReaderEntry(DiskMapReader owner, byte[] key) {
      this.key = key;
      this.owner = owner;
    }
    @Override
    public byte[] getKey() {
      return key;
    }

    @Override
    public byte[] getValue() {
      return owner.get(key);
    }

    @Override
    public byte[] setValue(byte[] value) {
      throw new UnsupportedOperationException("Read only.");
    }
    
    @Override
    public int hashCode() {
      return CmpUtil.hash(key);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final DiskMapReaderEntry other = (DiskMapReaderEntry) obj;
      if (!Arrays.equals(this.key, other.key)) {
        return false;
      }
      return true;
    }
    
  }
}
