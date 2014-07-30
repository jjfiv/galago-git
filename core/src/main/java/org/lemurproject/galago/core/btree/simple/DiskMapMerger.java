// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DiskMapMerger {
  public static DiskMapReader merge(String outputPath, List<String> inputPaths) throws IOException {
    DiskMapSortedBuilder mb = new DiskMapSortedBuilder(outputPath, Parameters.instance());
    
    ArrayList<DiskMapReader> readers = new ArrayList<>();
    for(String in : inputPaths) {
      readers.add(new DiskMapReader(in));
    }
    
    ArrayList<byte[]> keys = new ArrayList<>();
    for(DiskMapReader rdr : readers) {
      keys.addAll(rdr.keySet());
    }
    
    Collections.sort(keys, new CmpUtil.ByteArrComparator());
    
    for(byte[] key : keys) {
      for(DiskMapReader rdr : readers) {
        byte[] value = rdr.get(key);
        if(value != null) {
          mb.put(key, value);
          break;
        }
      }
    }
    
    mb.close();
    return new DiskMapReader(outputPath);
  }
  
  public static DiskMapReader mergeWith(String outputPath, List<String> inputPaths, MergeStrategy mergeFn) throws IOException {
    DiskMapSortedBuilder mb = new DiskMapSortedBuilder(outputPath, Parameters.instance());
    
    ArrayList<DiskMapReader> readers = new ArrayList<>();
    for(String in : inputPaths) {
      readers.add(new DiskMapReader(in));
    }
    
    ArrayList<byte[]> keys = new ArrayList<>();
    for(DiskMapReader rdr : readers) {
      keys.addAll(rdr.keySet());
    }
    
    Collections.sort(keys, new CmpUtil.ByteArrComparator());
    
    for(byte[] key : keys) {
      ArrayList<byte[]> values = new ArrayList<>();
      for(DiskMapReader rdr : readers) {
        byte[] value = rdr.get(key);
        if(value != null) {
          values.add(value);
        }
      }
      if(values.size() == 1) {
        mb.put(key, values.get(0));
      } else {
        mb.put(key, mergeFn.merge(values));
      }
    }
    
    mb.close();
    return new DiskMapReader(outputPath);
  }
  
  public static interface MergeStrategy {
    public byte[] merge(List<byte[]> inputs);
  }
  public static final class TakeFirst implements MergeStrategy {
    @Override
    public byte[] merge(List<byte[]> inputs) {
      return inputs.get(0);
    }
  }
}
