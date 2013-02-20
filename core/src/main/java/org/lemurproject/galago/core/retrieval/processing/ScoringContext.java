// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator;
import org.lemurproject.galago.core.retrieval.query.Node;

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
  protected HashMap<String, MovableLengthsIterator> lengths;
  protected TObjectIntHashMap<String> current;
  // indicates when nodes can/can't cache data
  // -- useful for passage or extent retrieval.
  public boolean cachable = true;

  // Diagnostic
  public HashMap<MovableIterator, Node> toNodes = new HashMap<MovableIterator, Node>();
  
  public ScoringContext() {
    lengths = new HashMap<String, MovableLengthsIterator>();
    current = new TObjectIntHashMap<String>();
  }

  public void addLength(String key, MovableLengthsIterator iterator) {
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
      for (Map.Entry<String, MovableLengthsIterator> pair : lengths.entrySet()) {
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
