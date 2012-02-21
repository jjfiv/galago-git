// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.Serializable;

/**
 * Basic retrieval unit. The results returned by the Retrieval.runQuery typically return
 * a ranked list of these.
 *
 * @author trevor, irmarc, sjh
 */
public class ScoredDocument implements Comparable<ScoredDocument>, Serializable {

  public ScoredDocument() {
    this(0, 0);
  }

  public ScoredDocument(int document, double score) {
    this.document = document;
    this.score = score;
  }

  public ScoredDocument(String documentName, int rank, double score) {
    this.documentName = documentName;
    this.rank = rank;
    this.score = score;
    this.document = -1;
  }
  
  @Override
  public int compareTo(ScoredDocument other) {
    if (score != other.score) {
      return Double.compare(score, other.score);
    }
    if( (source != null) && (other.source != null) &&
        (! source.equals(other.source))) {
      return source.compareTo(other.source);
    }
    return other.document - document;
  }

  @Override
  public String toString() {
    return String.format("%d,%f", document, score);
  }

  public String documentName;
  public String source; // lets us know where this scored doc came from
  public int document;
  public int rank;
  public double score;
}
