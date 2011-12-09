// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * #filter ( AbstractIndicator ScoreIterator ) : Only scores documents that where the AbstractIndicator is on
 *
 * 8/23/2011 (irmarc) - Changed this to an abstract class to implement most of what's needed for
 * a filter. RequireIterator (require) and RejectIterator (reject) now finish the job up.
 *
 * @author sjh
 */
public abstract class FilteredIterator implements CountValueIterator, ScoreValueIterator, ExtentValueIterator, ContextualIterator {

  protected ScoringContext context;
  protected IndicatorIterator indicator;
  protected CountValueIterator counter;
  protected ScoreValueIterator scorer;
  protected ExtentValueIterator extents;
  protected ValueIterator mover;
  protected boolean sharedChildren;

  public FilteredIterator(Parameters globalParams, NodeParameters parameters, IndicatorIterator indicator, CountValueIterator counter) {
    this.sharedChildren = globalParams.get("shareNodes", false);
    this.indicator = indicator;
    this.scorer = null;
    this.counter = counter;
    this.mover = counter;
    if (ExtentValueIterator.class.isAssignableFrom(counter.getClass())) {
      this.extents = (ExtentValueIterator) counter;
    } else {
      this.extents = null;
    }
  }

  public FilteredIterator(Parameters globalParams, NodeParameters parameters, IndicatorIterator indicator, ScoreValueIterator scorer) {
    this.sharedChildren = globalParams.get("shareNodes", false);
    this.indicator = indicator;
    this.counter = null;
    this.extents = null;
    this.scorer = scorer;
    this.mover = scorer;
  }

  public FilteredIterator(Parameters globalParams, NodeParameters parameters, IndicatorIterator indicator, ExtentValueIterator extents) {
    this.sharedChildren = globalParams.get("shareNodes", false);
    this.indicator = indicator;
    this.scorer = null;
    this.extents = extents;
    this.mover = extents;
    // Try to treat it as a counter if possible
    if (CountValueIterator.class.isAssignableFrom(extents.getClass())) {
      this.counter = (CountValueIterator) extents;
    } else {
      this.counter = null;
    }
  }

  public void reset() throws IOException {
    indicator.reset();
    mover.reset();
  }

  public ExtentArray extents() {
    return extents.extents();
  }

  public ExtentArray getData() {
    return extents.getData();
  }

  public int count() {
    return counter.count();
  }

  public double score() {
    return scorer.score();
  }

  public double score(ScoringContext context) {
    return scorer.score(context);
  }

  public double maximumScore() {
    return scorer.maximumScore();
  }

  public double minimumScore() {
    return scorer.minimumScore();
  }

  public ScoringContext getContext() {
    return context;
  }

  public void setContext(ScoringContext context) {
    this.context = context;
  }

  public boolean isDone() {
    return mover.isDone();
  }

  public int currentCandidate() {
    return mover.currentCandidate();
  }

  public boolean next() throws IOException {
    return moveTo(currentCandidate() + 1);
  }

  public void movePast(int identifier) throws IOException {
    moveTo(identifier + 1);
  }

  public boolean moveTo(int identifier) throws IOException {
    if (!isDone()) {
      indicator.moveTo(identifier);
      mover.moveTo(identifier);
      if (!sharedChildren) {
        findBestCandidate();
      }
    }
    return !isDone();
  }

  // Stops the iterators on the first document
  // that passes the indicator and is contained by
  // the mover list
  // -- this is the aggressive option
  private void findBestCandidate() throws IOException {
    int lowestBest = 0;
    while (!isDone()) {
      lowestBest = Math.max(lowestBest, indicator.currentCandidate());
      lowestBest = Math.max(lowestBest, mover.currentCandidate());
      indicator.moveTo(lowestBest);
      mover.moveTo(lowestBest);
      if (this.hasMatch(lowestBest)) {
        return;
      }
      // ensure we progress.
      lowestBest += 1;
    }
  }

  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Filter nodes don't have singular values");
  }

  public long totalEntries() {
    return Math.min(indicator.totalEntries(), mover.totalEntries());
  }

  public int compareTo(ValueIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentCandidate() - other.currentCandidate();
  }
}
