// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 * #filter ( AbstractIndicator ScoreIterator ) : Only scores documents that
 * where the AbstractIndicator is on
 *
 * 8/23/2011 (irmarc) - Changed this to an abstract class to implement most of
 * what's needed for a filter. RequireIterator (require) and RejectIterator
 * (reject) now finish the job up.
 *
 * @author irmarc, sjh
 */
public abstract class FilteredIterator extends ConjunctionIterator implements CountIterator, MovableScoreIterator, MovableExtentIterator {

  protected MovableIndicatorIterator indicator;
  protected CountIterator counter;
  protected MovableScoreIterator scorer;
  protected MovableExtentIterator extents;
  protected MovableIterator mover;

  public FilteredIterator(NodeParameters parameters, MovableIndicatorIterator indicator, CountIterator counter) {
    super(parameters, new MovableIterator[]{ indicator, counter });
    this.indicator = indicator;
    this.scorer = null;
    this.counter = counter;
    this.mover = counter;
    if (MovableExtentIterator.class.isAssignableFrom(counter.getClass())) {
      this.extents = (MovableExtentIterator) counter;
    } else {
      this.extents = null;
    }
  }

  public FilteredIterator(NodeParameters parameters, MovableIndicatorIterator indicator, MovableScoreIterator scorer) {
    super(parameters, new MovableIterator[]{ indicator, scorer });
    this.indicator = indicator;
    this.counter = null;
    this.extents = null;
    this.scorer = scorer;
    this.mover = scorer;
  }

  public FilteredIterator(NodeParameters parameters, MovableIndicatorIterator indicator, MovableExtentIterator extents) {
    super(parameters, new MovableIterator[]{ indicator, extents });
    this.indicator = indicator;
    this.scorer = null;
    this.extents = extents;
    this.mover = extents;
    // Try to treat it as a counter if possible
    if (CountIterator.class.isAssignableFrom(extents.getClass())) {
      this.counter = (CountIterator) extents;
    } else {
      this.counter = null;
    }
  }

  @Override
  public ExtentArray extents() {
    return extents.extents();
  }

  @Override
  public ExtentArray getData() {
    return extents.getData();
  }

  @Override
  public int count() {
    return counter.count();
  }

  @Override
  public int maximumCount() {
    return counter.maximumCount();
  }

  @Override
  public double score() {
    return scorer.score();
  }

  @Override
  public double maximumScore() {
    return scorer.maximumScore();
  }

  @Override
  public double minimumScore() {
    return scorer.minimumScore();
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Filter nodes don't have singular values");
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    List<AnnotatedNode> children = new ArrayList();
    children.add(indicator.getAnnotatedNode());
    children.add(mover.getAnnotatedNode());

    String type = "unknown";
    String returnValue = "unknown";
    if (this.counter != null) {
      type = "count";
      returnValue = Integer.toString(count());
    } else if (this.scorer != null) {
      type = "score";
      returnValue = Double.toString(score());
    } else if (this.counter != null) {
      type = "extents";
      returnValue = extents().toString();
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
