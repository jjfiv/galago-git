// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility;

import java.lang.ref.WeakReference;
import java.util.List;
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
  WeakHashMap<String, WeakReference<String>> pool = new WeakHashMap<>();

	static StringPooler _instance = new StringPooler();
	public static StringPooler getInstance() {
		return _instance;
	}

  /** Disable string pooling; probably not needed unless you're in a huge drmaa job. */
  public static void disable() {
    _instance = new StringPooler() {
      @Override public void transform(List<String> terms) { }
    };
  }

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
   * @param terms are the list of terms, probably from the document.
   */
  public void transform(List<String> terms) {
    if (maxActive > 0 && pool.size() > maxActive) {
      pool.clear();
    }

    for (int i = 0; i < terms.size(); i++) {
      String term = terms.get(i);

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
          terms.set(i, term);
          continue;
        }
      }

      // otherwise the pool does not contain the term - gc or new term
      term = new String(term);
      pool.put(term, new WeakReference<>(term));
      // still want to set the term to the newly cached term
      terms.set(i, term);
    }
  }
}
