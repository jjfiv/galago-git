/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.btree.BTreeReader;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sjh
 */
public class DiskNameSource extends BTreeKeySource implements DataSource<String> {

  public DiskNameSource(BTreeReader rdr) throws IOException {
    super(rdr);
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public String key() {
    return "names";
  }

  @Override
  public boolean hasMatch(long id) {
    return (!isDone() && currentCandidate() == id);
  }

  @Override
  public String data(long id) {
    try {
      if (currentCandidate() == id) {
        return ByteUtil.toString(btreeIter.getValueBytes());
      }
    } catch (IOException ex) {
      Logger.getLogger(DiskNameSource.class.getName()).log(Level.SEVERE, "Failed to deserialize document name for " + id, ex);
    }
    return null;
  }
}
