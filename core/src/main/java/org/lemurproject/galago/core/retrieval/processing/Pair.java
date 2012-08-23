/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

/**
 *
 * @author irmarc
 */
public class Pair implements Comparable<Pair> {

  public Pair(int d, int c) {
    doc = d;
    count = c;
  }
  public int doc;
  public int count;

  @Override
  public int compareTo(Pair that) {
    return this.doc - that.doc;
  }
  

}
