// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.util.Comparator;

/**
 * An extension to the ScoredDocument class, to include a begin and end over
 * a given document. Using these, a unique result is determined by {document,begin,end}.
 *
 * @author irmarc
 */
public class ScoredPassage extends ScoredDocument {

  public int begin;
  public int end;

  public ScoredPassage() {
    super(0, 0);
    this.begin = this.end = 0;
  }

  public ScoredPassage(long document, double score) {
    super(document, score);
    this.begin = this.end = 0;
  }

  public ScoredPassage(long document, double score, int begin, int end) {
    this(document, score);
    this.begin = begin;
    this.end = end;
  }

  public int compareTo(ScoredPassage other) {
    int result = super.compareTo(other);

    if (result != 0) {
      return result;
    }

    return other.begin - begin;
  }

  @Override
  public ScoredPassage clone(double score) {
    ScoredPassage psg = new ScoredPassage(this.document, this.score, this.begin, this.end);
    psg.documentName = this.documentName;
    psg.source = this.source;
    psg.rank = this.rank;
    psg.annotation = this.annotation;
    return psg;
  }

  @Override
  public String toString() {
    return String.format("%s %d %s %d %d", documentName, rank, formatScore(score), begin, end);
  }

  public String toString(String qid) {
    return String.format("%s Q0 %s %d %s galago %d %d", qid, documentName, rank, formatScore(score), begin, end);
  }

  @Override
  public String toTRECformat(String qid) {
    return String.format("%s Q0 %s %d %s galago", qid, documentName, rank, formatScore(score));
  }

  public static class ScoredPassageComparator implements Comparator<ScoredPassage> {

    @Override
    public int compare(ScoredPassage o1, ScoredPassage o2) {
        return o1.compareTo(o2);
    }
  }
}
