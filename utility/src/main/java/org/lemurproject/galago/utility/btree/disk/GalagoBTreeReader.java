package org.lemurproject.galago.utility.btree.disk;

import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.utility.btree.disk.VocabularyReader;

/**
 * @author jfoley.
 */
public abstract class GalagoBTreeReader extends BTreeReader {
  /**
   * Returns the vocabulary structure for this DiskBTreeReader. - Note that the
   * vocabulary contains only the first key in each block.
   */
  public abstract VocabularyReader getVocabulary();
}
