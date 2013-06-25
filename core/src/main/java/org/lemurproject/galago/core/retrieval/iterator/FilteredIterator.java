// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

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
public abstract class FilteredIterator extends ConjunctionIterator implements CountIterator, ScoreIterator, ExtentIterator {

  protected IndicatorIterator indicatorItr;
  protected CountIterator counter;
  protected ScoreIterator scorer;
  protected ExtentIterator extents;
  protected BaseIterator mover;
  protected ExtentArray emptyExtentArray = new ExtentArray();

  public FilteredIterator(NodeParameters parameters, IndicatorIterator indicator, CountIterator counter) {
    super(parameters, new BaseIterator[]{indicator, counter});
    this.indicatorItr = indicator;
    this.scorer = null;
    this.counter = counter;
    this.mover = counter;
    if (ExtentIterator.class.isAssignableFrom(counter.getClass())) {
      this.extents = (ExtentIterator) counter;
    } else {
      this.extents = null;
    }
  }

  public FilteredIterator(NodeParameters parameters, IndicatorIterator indicator, ScoreIterator scorer) {
    super(parameters, new BaseIterator[]{indicator, scorer});
    this.indicatorItr = indicator;
    this.counter = null;
    this.extents = null;
    this.scorer = scorer;
    this.mover = scorer;
  }

  public FilteredIterator(NodeParameters parameters, IndicatorIterator indicator, ExtentIterator extents) {
    super(parameters, new BaseIterator[]{indicator, extents});
    this.indicatorItr = indicator;
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
  public ExtentArray extents(ScoringContext c) {
    if (indication(c)) {
      return extents.extents(c);
    } else {
      return emptyExtentArray;
    }
  }

  @Override
  public ExtentArray data(ScoringContext c) {
    return extents(c);
  }

  @Override
  public int count(ScoringContext c) {
    if (indication(this.context)) {
      return counter.count(c);
    } else {
      return 0;
    }
  }

  @Override
  public double score(ScoringContext c) {
    if (indication(c)) {
      return scorer.score(c);
    } else {
      return Utility.tinyLogProbScore;
    }
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
  public String getValueString(ScoringContext c) throws IOException {
    throw new UnsupportedOperationException("Filter nodes don't have singular values");
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    List<AnnotatedNode> children = new ArrayList();
    children.add(indicatorItr.getAnnotatedNode(c));
    children.add(mover.getAnnotatedNode(c));

    String type = "unknown";
    String returnValue = "unknown";
    if (this.counter != null) {
      type = "count";
      returnValue = Integer.toString(count(c));
    } else if (this.scorer != null) {
      type = "score";
      returnValue = Double.toString(score(c));
    } else if (this.counter != null) {
      type = "extents";
      returnValue = extents(c).toString();
    }
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  // This function returns true if the filter should accept the value (see require/reject iterator)
  protected abstract boolean indication(ScoringContext context);
}
