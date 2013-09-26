// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.Serializable;
import java.util.Comparator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Basic retrieval unit. The results returned by the Retrieval.runQuery typically return
 * a ranked list of these.
 *
 * @author trevor, irmarc, sjh
 */
public class ScoredDocument implements Comparable<ScoredDocument>, Serializable {

  public String documentName;
  public String source; // lets us know where this scored doc came from
  public long document;
  public int rank;
  public double score;
  public AnnotatedNode annotation = null;

  public ScoredDocument() {
    this(0, 0);
  }

  public ScoredDocument(long document, double score) {
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
    
    int cmp = Utility.compare(score, other.score);
    if (cmp != 0) {
      return cmp;
    }

    if ((source != null) && (other.source != null)
            && (!source.equals(other.source))) {
      return source.compareTo(other.source);
    }
    return Utility.compare(other.document, document);
  }

  @Override
  public String toString() {
    return String.format("%s %d %s galago", documentName, rank, formatScore(score));
  }

  public String toString(String qid) {
    return String.format("%s Q0 %s %d %s galago", qid, documentName, rank, formatScore(score));
  }

  public String toTRECformat(String qid) {
    return String.format("%s Q0 %s %d %s galago", qid, documentName, rank, formatScore(score));
  }

  protected static String formatScore(double score) {
    double difference = Math.abs(score - (int) score);

    if (difference < 0.00001) {
      return Integer.toString((int) score);
    }
    return String.format("%10.8f", score);
  }

  public static class ScoredDocumentComparator implements Comparator<ScoredDocument> {

    @Override
    public int compare(ScoredDocument o1, ScoredDocument o2) {
        return o1.compareTo(o2);
    }
  }
}
