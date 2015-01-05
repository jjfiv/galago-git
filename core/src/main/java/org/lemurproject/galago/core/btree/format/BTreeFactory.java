/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.btree.format;

import org.lemurproject.galago.utility.btree.disk.DiskBTreeReader;
import org.lemurproject.galago.utility.btree.disk.GalagoBTreeReader;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author sjh
 */
public class BTreeFactory {

  /*
   * Static functions to open an index file or folder
   */
  public static GalagoBTreeReader getBTreeReader(String filePath) throws IOException {
    return getBTreeReader(new File(filePath));
  }

  public static GalagoBTreeReader getBTreeReader(File f) throws IOException {
    if (SplitBTreeReader.isBTree(f)) {
      return new SplitBTreeReader(f);
    } else if (DiskBTreeReader.isBTree(f)) {
      return new DiskBTreeReader(f);
    } else {
      return null;
    }
  }

  /**
   * Static functions to check if the path contains an index of some type
   */
  public static boolean isBTree(String filePath) throws IOException {
    return isBTree(new File(filePath));
  }

  public static boolean isBTree(File f) throws IOException {
    if (f.isFile() && DiskBTreeReader.isBTree(f)) {
      return true;
    }
    if (f.isDirectory() && SplitBTreeReader.isBTree(f)) {
      return true;
    }
    return false;
  }
}
