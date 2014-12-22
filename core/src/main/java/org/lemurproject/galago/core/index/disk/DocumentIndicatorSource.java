// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.btree.format.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.BooleanSource;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
public class DocumentIndicatorSource extends BTreeKeySource implements BooleanSource {

  public final boolean defaultValue;

  public DocumentIndicatorSource(BTreeReader rdr, boolean defaultValue) throws IOException {
    super(rdr);
    this.defaultValue = defaultValue;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public String key() {
    return "indicators";
  }

  /**
   * If the data stored is false, it's not really a match.
   *
   * Because even though we're in source world, we don't consider a stored false
   * to be worth "scoring".
   */
  @Override
  public boolean hasMatch(long id) {
    return (!isDone() && (currentCandidate() == id) && indicator(id));
  }

  @Override
  public boolean indicator(long id) {
    if (id == currentCandidate()) {
      try {
        byte[] data = btreeIter.getValueBytes();
        if (Utility.isBoolean(data)) {
          return Utility.toBoolean(data);
        }
        return defaultValue;
      } catch (IOException ioe) {
        Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ioe);
        throw new RuntimeException("Failed to read indicator file.");
      }
    }
    return false;
  }
}
