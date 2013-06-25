// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.source.ScoreSource;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author jfoley
 */
public class DiskScoreIterator extends SourceIterator implements ScoreIterator {

  ScoreSource scoreSrc;

  public DiskScoreIterator(ScoreSource src) {
    super(src);
    scoreSrc = src;
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return String.format("%s,%d,%f", getKeyString(), currentCandidate(), score(c));
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "scores";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = Collections.EMPTY_LIST;

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public double score(ScoringContext c) {
    return scoreSrc.score(c.document);
  }

  @Override
  public double maximumScore() {
    return scoreSrc.maxScore();
  }

  @Override
  public double minimumScore() {
    return scoreSrc.minScore();
  }
}
