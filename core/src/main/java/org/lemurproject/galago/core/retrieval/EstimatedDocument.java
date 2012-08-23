// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.util.Comparator;

/**
 * Augmented to track soft scoring
 *
 * @author irmarc
 */
public class EstimatedDocument extends ScoredDocument {

  public double min;
  public double max;
  public int length;
  public short[] counts;

  public EstimatedDocument() {
    this(0, 0);
  }

  public EstimatedDocument(int document, double score) {
    super(document, score);
    min = max = 0.0;
  }

  public EstimatedDocument(int document, double score, int len) {
    super(document, score);
    length = len;
    min = max = 0.0;
  }

  public EstimatedDocument(int document, double score, double lower, double upper) {
    super(document, score);
    min = lower;
    max = upper;
  }

  public EstimatedDocument(int document, double score, double lower, double upper, int l) {
    this(document, score, lower, upper);
    length = l;
  }

  public EstimatedDocument(int document, double score, double lower, double upper, int l,
          short[] c) {
    this(document, score, lower, upper);
    length = l;
    counts = new short[c.length];
    System.arraycopy(c, 0, counts, 0, c.length);
  }

  public EstimatedDocument(String documentName, int rank, double score) {
    super(documentName, rank, score);
    min = max = 0.0;
  }

  public EstimatedDocument(String documentName, int rank, double score, double lower, double upper) {
    super(documentName, rank, score);
    min = lower;
    max = upper;
  }

  /**
   * Implements same sort order as the MinComparator
   * 
   * @param other
   * @return
   */
  public int compareTo(EstimatedDocument other) {
    double thislow = score + min;
    double thatlow = other.score + other.min;
    if (thislow != thatlow) {
      return Double.compare(thatlow, thislow);
    }
    if (score != other.score) {
      return Double.compare(other.score, score);
    }
    if ((source != null) && (other.source != null)
            && (!source.equals(other.source))) {
      return other.source.compareTo(source);
    }
    return document - other.document;
  }

  public static class MinComparator implements Comparator<EstimatedDocument> {

    @Override
    public int compare(EstimatedDocument o1, EstimatedDocument o2) {
      double lo1 = o1.score + o1.min;
      double lo2 = o2.score + o2.min;
      if (lo1 != lo2) {
        return Double.compare(lo1, lo2);
      }
      if (o1.score != o2.score) {
        return Double.compare(o1.score, o2.score);
      }
      if ((o1.source != null) && (o2.source != null)
              && (!o1.source.equals(o2.source))) {
        return o2.source.compareTo(o1.source);
      }
      return o1.document - o2.document;
    }
  }

  public static class MaxComparator implements Comparator<EstimatedDocument> {

    @Override
    public int compare(EstimatedDocument o1, EstimatedDocument o2) {
      double hi1 = o1.score + o1.max;
      double hi2 = o2.score + o2.max;
      if (hi1 != hi2) {
        return Double.compare(hi1, hi2);
      }
      if (o1.score != o2.score) {
        return Double.compare(o1.score, o2.score);
      }
      if ((o1.source != null) && (o2.source != null)
              && (!o1.source.equals(o2.source))) {
        return o2.source.compareTo(o1.source);
      }
      return o1.document - o2.document;
    }
  }

  @Override
  public String toString() {
    return String.format("%d,%f, (%f,%f)", document, score, min, max);
  }
}
