// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

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

  public ScoredPassage(int document, double score) {
    super(document, score);
    this.begin = this.end = 0;
  }

  public ScoredPassage(int document, double score, int begin, int end) {
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
  public String toString() {
    return String.format("%d:%d-%d,%f", document, begin, end, score);
  }
}
