// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.LengthsReader;

/**
 * Currently represents the context that the entire query processor shares. This
 * is the most basic context we use.
 *
 * The lengths are generally managed from this construct.
 *
 * @author irmarc
 */
public class ScoringContext {

  public int document;
  protected HashMap<String, LengthsReader.LengthsIterator> lengths;
  protected TObjectIntHashMap<String> current;

  public ScoringContext() {
    lengths = new HashMap<String, LengthsReader.LengthsIterator>();
    current = new TObjectIntHashMap<String>();
  }

  public void addLength(String key, LengthsReader.LengthsIterator iterator) {
    lengths.put(key, iterator);
  }

  public int getPosition(String key) {
    return lengths.get(key).getCurrentIdentifier();
  }

  public int getPosition() {
    return lengths.get("").getCurrentIdentifier();
  }

  public int getLength(String key) {
    return current.get(key);
  }

  public int getLength() {
    return current.get("");
  }

  public void moveLengths(int position) {
    try {
      for (Map.Entry<String, LengthsReader.LengthsIterator> pair : lengths.entrySet()) {
        if (pair == null) {
          System.err.printf("Missing pair.\n");
        } else if (pair.getValue() == null) {
          System.err.printf("Missing value for key %s.\n", pair.getKey());
        } else {
          pair.getValue().syncTo(position);
          current.put(pair.getKey(), pair.getValue().getCurrentLength());
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
