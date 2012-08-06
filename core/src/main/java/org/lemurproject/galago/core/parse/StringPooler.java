// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.WeakHashMap;

/**
 * The point of this class is to replace strings in document objects with
 * already-used copies.  This can greatly reduce the amount of memory used
 * by the system.
 *
 * @author trevor, sjh
 */
public class StringPooler {

  private static long DEFAULT_ACTIVE = 100000;

  long maxActive;
  WeakHashMap<String, WeakReference<String>> pool = new WeakHashMap<String, WeakReference<String>>();

  public StringPooler() {
    this(DEFAULT_ACTIVE);
  }

  public StringPooler(long maxActive) {
    this.maxActive = maxActive;
  }

  /**
   * Replaces the strings within this document with strings in a
   * string pool.
   *
   * String pool is weakly referenced - it can be garbage collected
   *
   * @param document
   */
  public void transform(Document document) {
    if (maxActive > 0 && pool.size() > maxActive) {
      pool.clear();
    }

    for (int i = 0; i < document.terms.size(); i++) {
      String term = document.terms.get(i);

      if (term == null) {
        continue;
      }

      WeakReference<String> cacheRef = pool.get(term);
      if (cacheRef != null) {
        // if we have a response
        String cached = cacheRef.get();
        // first check that the string it holds has not been gc-ed between the last two lines of execution:
        if (cached != null) {
          // term was cached!
          term = cached;
          // now replace the doc reference to the cached string
          document.terms.set(i, term);
          continue;
        }
      }

      // otherwise the pool does not contain the term - gc or new term
      term = new String(term);
      pool.put(term, new WeakReference(term));
      // still want to set the term to the newly cached term
      document.terms.set(i, term);
    }
  }
}
