// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.BooleanSource;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
public class DocumentIndicatorSource extends BTreeKeySource implements BooleanSource  {
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
  
  @Override
  /**
   * If the data stored is false, it's not really a match.
   */
  public boolean hasMatch(long id) {
    return (!isDone() && (currentCandidate() == id) && indicator(id));
  }

  @Override
  public boolean indicator(long id) {
    if (id == currentCandidate()) {
      try {
        byte[] data = btreeIter.getValueBytes();
        if (data == null || data.length == 0) {
          System.err.println("Returning defaultValue="+defaultValue);
          return defaultValue;
        } else {
          System.err.println("Returning data="+Utility.toBoolean(data));
          return Utility.toBoolean(data);
        }
      } catch (IOException ioe) {
        Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ioe);
        throw new RuntimeException("Failed to read indicator file.");
      }
    }
    return false;
  }
}
