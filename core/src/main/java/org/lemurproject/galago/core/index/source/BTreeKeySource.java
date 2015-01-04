// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.source;

import java.io.IOException;

import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This class treats an empty BTree or at least the keys of a BTree as a data
 * source.
 *
 * The implementation sits mostly on top of the BTreeIterator, leaving very few
 * methods for subclasses.
 *
 * @author jfoley
 * @see DocumentNamesSource
 * @see DocumentCorpusSource
 * @see DocumentIndicatorSource
 * @see DocumentPriorSource
 */
public abstract class BTreeKeySource implements DiskSource {

  protected BTreeIterator btreeIter;
  protected BTreeReader btreeReader;
  public final long keyCount;

  public BTreeKeySource(BTreeReader rdr) throws IOException {
    btreeReader = rdr;
    keyCount = btreeReader.getManifest().get("keyCount", 0);
    reset();
  }

  /**
   * This fetches a new BTreeIterator from the BTreeReader.
   *
   * Implementing classes will need to call this reset explicitly if they want
   * to do anything remotely interesting in their own reset methods.
   *
   * @throws IOException
   */
  @Override
  public void reset() throws IOException {
    btreeIter = btreeReader.getIterator();
  }

  @Override
  public boolean isDone() {
    return btreeIter.isDone();
  }

  @Override
  public long currentCandidate() {
    return Utility.toLong(btreeIter.getKey());
  }

  @Override
  public void movePast(long id) throws IOException {
    syncTo(id + 1);
  }

  @Override
  public void syncTo(long id) throws IOException {
    if(isDone()) {
      return;
    }
    btreeIter.skipTo(Utility.fromLong(id));
  }

  @Override
  public long totalEntries() {
    return keyCount;
  }
}
