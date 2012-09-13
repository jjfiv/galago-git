// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

/**
 * A small wrapper around a document and a count. This is primarily used for caching
 * term counts during a two-pass execution.
 * 
 * @author irmarc
 */
public class Pair implements Comparable<Pair> {
  public int doc;
  public int count;
  
  public Pair(int d, int c) {
    doc = d;
    count = c;
  }

  /**
   * Orders by the doc field.
   * 
   * @param that
   * @return 
   */
  @Override
  public int compareTo(Pair that) {
    return this.doc - that.doc;
  }
}
