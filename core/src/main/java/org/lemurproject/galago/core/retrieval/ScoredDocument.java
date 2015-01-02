// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.lists.Ranked;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Basic retrieval unit. The results returned by the Retrieval.runQuery typically return
 * a ranked list of these.
 *
 * @author trevor, irmarc, sjh
 */
public class ScoredDocument extends Ranked implements EvalDoc, Comparable<ScoredDocument>, Serializable {

  public String documentName;
  public String source; // lets us know where this scored doc came from
  public long document;
  public AnnotatedNode annotation = null;

  public ScoredDocument() {
    this(0, 0);
  }

  public ScoredDocument(long document, double score) {
    super(0,score);
    this.document = document;
  }

  public ScoredDocument(String documentName, int rank, double score) {
    super(rank, score);
    this.documentName = documentName;
    this.document = -1;
  }

  @Override
  public int compareTo(ScoredDocument other) {
    int cmp = CmpUtil.compare(score, other.score);
    if (cmp != 0) {
      return cmp;
    }

    if ((source != null) && (other.source != null)
            && (!source.equals(other.source))) {
      return source.compareTo(other.source);
    }
    return CmpUtil.compare(other.document, document);
  }

  @Override
  public boolean equals(Object other) {
    if(other == null) return false;
    if(other == this) return true;
    return other instanceof ScoredDocument && compareTo((ScoredDocument) other) == 0;
  }

  @Override
  public int hashCode() {
    return (int) (this.source.hashCode() ^ this.documentName.hashCode() ^ rank ^ Double.doubleToLongBits(score) ^ this.document);
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

  @Override
  public ScoredDocument clone(double score) {
    ScoredDocument doc = new ScoredDocument();
    doc.score = score;
    doc.rank = this.rank;
    doc.documentName = this.documentName;
    doc.document = this.document;
    doc.source = this.source;
    doc.annotation = this.annotation;
    return doc;
  }

  @Override
  public int getRank() {
    return rank;
  }

  @Override
  public double getScore() {
    return score;
  }

  @Override
  public String getName() {
    return documentName;
  }

  public static class ScoredDocumentComparator implements Comparator<ScoredDocument>, Serializable {
    @Override
    public int compare(ScoredDocument o1, ScoredDocument o2) {
        return o1.compareTo(o2);
    }
  }


  public static class RankComparator implements Comparator<ScoredDocument>, Serializable {
    @Override
    public int compare(ScoredDocument o1, ScoredDocument o2) {
      return CmpUtil.compare(o1.rank, o2.rank);
    }
  }
}
