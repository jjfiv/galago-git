/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.index.disk.DiskBTreeReader;

/**
 *
 * @author sjh
 */
public class BTreeFactory {

  /*
   * Static function to open an index file or folder
   */
  public static BTreeReader getBTreeReader(String pathname) throws IOException {
    if (!pathname.contains(File.separator)) {
      pathname = "." + File.separator + pathname;
    }
    File f = new File(pathname);

    if (SplitBTreeReader.isBTree(f)) {
      return new SplitBTreeReader(f);
    } else if (DiskBTreeReader.isBTree(f)) {
      return new DiskBTreeReader(f);
    } else {
      return null;
    }
  }

  /**
   * Static function to check if the path contains an index of some type
   */
  public static boolean isBTree(String pathname) throws IOException {
    if (!pathname.contains(File.separator)) {
      pathname = "." + File.separator + pathname;
    }
    File f = new File(pathname);

    if (SplitBTreeReader.isBTree(f)) {
      return true;
    }
    if (DiskBTreeReader.isBTree(f)) {
      return true;
    }
    return false;
  }
}
