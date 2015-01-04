// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
public class DocumentPriorSource extends BTreeKeySource implements ScoreSource {

  final double maxScore;
  final double minScore;
  final double def;

  public DocumentPriorSource(BTreeReader rdr, double def) throws IOException {
    super(rdr);
    final Parameters manifest = btreeReader.getManifest();
    this.maxScore = manifest.get("maxScore", Double.POSITIVE_INFINITY);
    this.minScore = manifest.get("minScore", Double.NEGATIVE_INFINITY);
    this.def = def;
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public String key() {
    return "priors";
  }

  @Override
  public boolean hasMatch(long id) {
    return true;
  }

  @Override
  public double score(long id) {
    try {
      if (currentCandidate() == id) {
        byte[] valueBytes = btreeIter.getValueBytes();
        if (Utility.isDouble(valueBytes)) {
          return Utility.toDouble(valueBytes);
        }
      }
      return def;
    } catch (IOException ioe) {
      Logger.getLogger(DocumentPriorSource.class.getName()).log(Level.SEVERE, null, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public double maxScore() {
    return maxScore;
  }

  @Override
  public double minScore() {
    return minScore;
  }
}
