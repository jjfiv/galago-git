package org.lemurproject.galago.core.btree.simple;

import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ReadOnlyMap;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author jfoley
 */
public class DiskMapWrapper<KT, VT> extends ReadOnlyMap<KT, VT> implements Closeable {
  public final DiskMapReader reader;
  private final Codec<KT> keyCodec;
  private final Codec<VT> valCodec;

  public DiskMapWrapper(DiskMapReader reader, Codec<KT> keyCodec, Codec<VT> valCodec) {
    this.reader = reader;
    this.keyCodec = keyCodec;
    this.valCodec = valCodec;
  }
  public DiskMapWrapper(String path, Codec<KT> keyCodec, Codec<VT> valCodec) throws IOException {
    this(new DiskMapReader(path), keyCodec, valCodec);
  }
  public DiskMapWrapper(File path, Codec<KT> keyCodec, Codec<VT> valCodec) throws IOException {
    this(new DiskMapReader(path.getAbsolutePath()), keyCodec, valCodec);
  }

  @Override
  public int size() {
    return reader.size();
  }

  @Override
  public boolean isEmpty() {
    return reader.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    byte[] key = keyCodec.toBytes((KT) o);
    return reader.containsKey(key);
  }

  @Override
  public boolean containsValue(Object o) {
    byte[] value = valCodec.toBytes((VT) o);
    return reader.containsValue(value);
  }

  @Override
  public VT get(Object o) {
    byte[] val = reader.get(keyCodec.toBytes((KT) o));
    if(val == null) return null;
    return valCodec.fromBytes(val);
  }

  @Override
  public Set<KT> keySet() {
    Set<KT> data = new HashSet<KT>();
    for(byte[] key : reader.keySet()) {
      data.add(keyCodec.fromBytes(key));
    }
    return data;
  }

  @Override
  public Collection<VT> values() {
    ArrayList<VT> vals = new ArrayList<VT>();
    for(byte[] b : reader.values()) {
      vals.add(valCodec.fromBytes(b));
    }
    return vals;
  }

  @Override
  public Set<Entry<KT, VT>> entrySet() {
    Set<Entry<KT,VT>> data = new HashSet<Entry<KT,VT>>();
    for(Entry<byte[], byte[]> e : reader.entrySet()) {
      data.add(new AbstractMap.SimpleImmutableEntry<KT,VT>(
        keyCodec.fromBytes(e.getKey()),
        valCodec.fromBytes(e.getValue())
      ));
    }
    return data;
  }

  public void forEach(IAction<KT, VT> action) {
    try {
      BTreeReader.BTreeIterator iterator = reader.btree.getIterator();
      while(!iterator.isDone()) {
        KT key = keyCodec.fromBytes(iterator.getKey());
        VT value = valCodec.fromBytes(iterator.getValueBytes());
        action.process(key, value);
        iterator.nextKey();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<KT,VT> bulkGet(Collection<KT> keys) {
    ArrayList<byte[]> rawKeys = new ArrayList<byte[]>(keys.size());
    for(KT key : keys) {
      rawKeys.add(keyCodec.toBytes(key));
    }
    Map<KT,VT> output = new HashMap<KT,VT>(rawKeys.size());

    Collections.sort(rawKeys, new CmpUtil.ByteArrComparator());
    try {
      BTreeReader.BTreeIterator iterator = reader.btree.getIterator();
      for (byte[] query : rawKeys) {
        iterator.skipTo(query);

        byte[] actual = iterator.getKey();
        if (actual == null || !CmpUtil.equals(actual, query)) {
          continue;
        }
        KT key = keyCodec.fromBytes(actual);
        VT value = valCodec.fromBytes(iterator.getValueBytes());
        output.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return output;
  }

  @Override
  public void close() throws IOException {
    reader.btree.close();
  }

  public static interface Codec<T> {
    public T fromBytes(byte[] in);
    public byte[] toBytes(T out);
  }
  public static final class StringCodec implements Codec<String> {
    @Override
    public String fromBytes(byte[] in) {
      return ByteUtil.toString(in);
    }

    @Override
    public byte[] toBytes(String out) {
      return ByteUtil.fromString(out);
    }
  }
  public static final class IntCodec implements Codec<Integer> {
    @Override
    public Integer fromBytes(byte[] in) {
      return Utility.toInt(in);
    }

    @Override
    public byte[] toBytes(Integer out) {
      return Utility.fromInt(out);
    }
  }
  public static final class JSONCodec implements Codec<Parameters> {
    @Override
    public Parameters fromBytes(byte[] in) {
      try {
        return Parameters.parseString(ByteUtil.toString(in));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public byte[] toBytes(Parameters out) {
      return ByteUtil.fromString(out.toString());
    }
  }

  public static interface IAction<KT, VT> {
    public void process(KT key, VT value);
  }

}
